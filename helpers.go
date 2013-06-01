package main

import (
    "database/sql"
    "github.com/bmizerany/pq"
    "github.com/rcrowley/go-metrics"
    "github.com/streadway/amqp"
    "github.com/timonv/pusher"
    "log"
    "os"
    "time"
    //"strings"
)

var DB *sql.DB
var JobIdFind *sql.Stmt
var LogPartsInsert *sql.Stmt
var AMQP *amqp.Connection
var PusherClient *pusher.Client
var Metrics *metrics.StandardRegistry

func metricsLogging(interval int) {
    l := log.New(os.Stdout, "metrics: ", 0)

    for {
        now := time.Now().Unix()
        Metrics.Each(func(name string, i interface{}) {
            switch m := i.(type) {
            case metrics.Counter:
                l.Printf("time=%d name=%s type=count count=%d\n", now, name, m.Count())
            case metrics.Gauge:
                l.Printf("time=%d name=%s type=gauge value=%d\n", now, name, m.Value())
            case metrics.Healthcheck:
                m.Check()
                l.Printf("time=%d name=%s type=healthcheck error=%v\n", now, name, m.Error())
            case metrics.Histogram:
                ps := m.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
                l.Printf("time=%d name=%s type=histogram count=%d min=%d max=%d mean=%f stddev=%f median=%f 95th_percentile=%f 99th_percentile=%f\n", now, name, m.Count(), m.Min(), m.Max(), m.Mean(), m.StdDev(), ps[0], ps[2], ps[3])
            case metrics.Meter:
                l.Printf("time=%d name=%s type=meter count=%d one_minute_rate=%f five_minute_rate=%f fifteen_minute_rate=%f mean_rate=%f\n", now, name, m.Count(), m.Rate1(), m.Rate5(), m.Rate15(), m.RateMean())
            case metrics.Timer:
                ps := m.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999})
                l.Printf("time=%d name=%s type=timer count=%d one_minute_rate=%f five_minute_rate=%f fifteen_minute_rate=%f mean_rate=%f min=%d max=%d mean=%f stddev=%f median=%f 95th_percentile=%f 99th_percentile=%f\n", now, name, m.Count(), m.Rate1(), m.Rate5(), m.Rate15(), m.RateMean(), m.Min(), m.Max(), m.Mean(), m.StdDev(), ps[0], ps[2], ps[3])
            }
        })
        time.Sleep(time.Duration(int64(1e9) * int64(interval)))
    }
}

func setupPusher() {
    key := os.Getenv("PUSHER_KEY")
    if key == "" {
        log.Fatalf("PUSHER_KEY was empty")
    }

    secret := os.Getenv("PUSHER_SECRET")
    if secret == "" {
        log.Fatalf("PUSHER_SECRET was empty")
    }

    appId := os.Getenv("PUSHER_APP_ID")
    if appId == "" {
        log.Fatalf("PUSHER_APP_ID was empty")
    }

    PusherClient = pusher.NewClient(appId, key, secret, false)
}

func setupDatabase() {
    log.Println("Setting up the Database")
    dbEnv := os.Getenv("DATABASE_URL")
    if dbEnv == "" {
        log.Fatalf("DATABASE_URL was empty")
    }

    pgUrl, err := pq.ParseURL(dbEnv)
    if err != nil {
        log.Fatalf("[database:parse] %s", err)
    }

    DB, err = sql.Open("postgres", pgUrl)
    if err != nil {
        log.Fatalf("[database:open] %s", err)
    }

    if err = DB.Ping(); err != nil {
        log.Fatalf("[database:ping] %s", err)
    }

    JobIdFind, err = DB.Prepare("SELECT id FROM logs WHERE job_id=$1")
    if err != nil {
        log.Fatalf("[database:prepare:jobidfind] %s", err)
    }

    LogPartsInsert, err = DB.Prepare("INSERT INTO log_parts (log_id, number, content, final, created_at) VALUES ($1, $2, $3, $4, $5) RETURNING id")
    if err != nil {
        log.Fatalf("[database:prepare:logpartsinsert] %s", err)
    }
}

func closeDatabase() {
    DB.Close()
}

func setupAMQP() {
    var err error

    url := os.Getenv("AMQP_URL")
    if url == "" {
        log.Fatal("We Haz No AMQP Deets")
    }

    log.Println("Connecting to AMQP")
    AMQP, err = amqp.Dial(url)
    if err != nil {
        log.Fatalf("connection.open: %s", err)
    }

    ch, err := AMQP.Channel()
    if err != nil {
        log.Fatalf("channel.open: %s", err)
    }

    if _, err := ch.QueueDeclare("reporting.jobs.logs", true, false, false, false, nil); err != nil {
        log.Fatalf("queue.declare: %s", err)
    }

    err = ch.ExchangeDeclare("reporting", "topic", true, false, false, false, nil)
    if err != nil {
        log.Fatalf("exchange.declare: %s", err)
    }

    ch.Close()
}

func closeAMQP() {
    AMQP.Close()
}

func subscribeToQueue(queueName string) (<-chan amqp.Delivery, *amqp.Channel) {
    log.Printf("Subscribing to %s", queueName)

    ch, err := AMQP.Channel()
    if err != nil {
        log.Fatalf("channel.open: %s", err)
    }

    err = ch.Qos(10, 0, false)
    if err != nil {
        log.Fatalf("channel.qos: %s", err)
    }

    messages, err := ch.Consume(queueName, "processor", false, false, false, false, nil)
    if err != nil {
        log.Fatalf("basic.consume: %v", err)
    }

    return messages, ch
}
