package main

import (
    "database/sql"
    "github.com/bmizerany/pq"
    "github.com/streadway/amqp"
    "github.com/timonv/pusher"
    "log"
    "os"
    //"strings"
)

var DB *sql.DB
var JobIdFind *sql.Stmt
var LogPartsInsert *sql.Stmt
var AMQP *amqp.Connection
var PusherClient *pusher.Client

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
