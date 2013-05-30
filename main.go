package main

import (
    "database/sql"
    "encoding/json"
    "fmt"
    "github.com/bmizerany/pq"
    "github.com/streadway/amqp"
    "github.com/timonv/pusher"
    "log"
    "os"
    //"strings"
    "sync"
    "time"
)

type Payload struct {
    JobId   int    `json:"id"`
    Number  int    `json:"number"`
    Content string `json:"log"`
    Final   bool   `json:"final"`
    UUID    string `json:"uuid"`
}

type PusherPayload struct {
    JobId   int    `json:"id"`
    Number  int    `json:"number"`
    Content string `json:"_log"`
    Final   bool   `json:"final"`
}

var Conn *sql.DB
var JobIdFind *sql.Stmt
var LogPartsInsert *sql.Stmt
var AMQP *amqp.Connection
var PusherClient *pusher.Client

func main() {
    setupAMQP()
    defer closeAMQP()

    setupDatabase()
    defer closeDatabase()

    PusherClient = pusher.NewClient("5028", "61c3b5fb2fe3f7833030", "dd3f5586214154941a04", false)

    logParts, _ := subscribeToLogs()

    var wg sync.WaitGroup
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func(logParts <-chan amqp.Delivery) {
            processLogParts(logParts)
            defer wg.Done()
        }(logParts)
    }
    wg.Wait()
}

func processLogParts(logParts <-chan amqp.Delivery) {
    for part := range logParts {
        //fmt.Printf("\n\n%#v\n", string(part.Body))
        var payload Payload
        json.Unmarshal(part.Body, &payload)
        //payload.Content = strings.Replace(payload.Content, "\x00", "", -1)
        fmt.Printf("job_id:%d number:%d\n", payload.JobId, payload.Number)
        //fmt.Printf("%#v\n", payload.Content)

        var logId string
        err := JobIdFind.QueryRow(payload.JobId).Scan(&logId)

        switch {
        case err == sql.ErrNoRows:
            log.Fatalf("No log with job_id:%s", payload.JobId)
        case err != nil:
            log.Fatalf("db.queryrow: %v", err)
        }

        var logPartId string
        err = LogPartsInsert.QueryRow(logId, payload.Number, payload.Content, payload.Final, time.Now()).Scan(&logPartId)

        switch {
        case err == sql.ErrNoRows:
            log.Fatalf("No new log part created")
        case err != nil:
            log.Fatalf("db.queryrow: %v", err)
        }

        // log.Println("Log Part created with id:", logPartId)
        liveStream(payload)

        part.Ack(false)
    }
}

func liveStream(payload Payload) {
    pusherPayload := PusherPayload{
        JobId:   payload.JobId,
        Number:  payload.Number,
        Content: payload.Content,
        Final:   payload.Final,
    }

    readyToPush, err := json.Marshal(pusherPayload)
    if err != nil {
        log.Fatalf("json.marshal: %v", err)
    }

    err = PusherClient.Publish(string(readyToPush), "job:log", fmt.Sprintf("job-%d", payload.JobId))
    if err != nil {
        log.Fatalf("pusherclient.publish: %v", err)
    }
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

    db, err := sql.Open("postgres", pgUrl)
    if err != nil {
        log.Fatalf("[database:open] %s", err)
    }

    if err = db.Ping(); err != nil {
        log.Fatalf("[database:ping] %s", err)
    }

    JobIdFind, err = db.Prepare("SELECT id FROM logs WHERE job_id=$1")
    if err = db.Ping(); err != nil {
        log.Fatalf("[database:prepare:jobidfind] %s", err)
    }

    LogPartsInsert, err = db.Prepare("INSERT INTO log_parts (log_id, number, content, final, created_at) VALUES ($1, $2, $3, $4, $5) RETURNING id")
    if err = db.Ping(); err != nil {
        log.Fatalf("[database:prepare:logpartsinsert] %s", err)
    }

    Conn = db
}

func closeDatabase() {
    Conn.Close()
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

func subscribeToLogs() (<-chan amqp.Delivery, *amqp.Channel) {
    log.Println("Subscribing to reporting.jobs.logs")

    ch, err := AMQP.Channel()
    if err != nil {
        log.Fatalf("channel.open: %s", err)
    }

    err = ch.Qos(10, 0, false)
    if err != nil {
        log.Fatalf("channel.qos: %s", err)
    }

    logParts, err := ch.Consume("reporting.jobs.logs", "processor", false, false, false, false, nil)
    if err != nil {
        log.Fatalf("basic.consume: %v", err)
    }

    return logParts, ch
}
