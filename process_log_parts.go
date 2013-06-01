package main

import (
    "database/sql"
    "encoding/json"
    "fmt"
    "github.com/rcrowley/go-metrics"
    "github.com/streadway/amqp"
    "log"
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

var MetricsProcessTimer *metrics.StandardTimer
var MetricsProcessFailedCount *metrics.StandardMeter
var MetricsPusherTimer *metrics.StandardTimer
var MetricsPusherFailedCount *metrics.StandardMeter

func ProcessLogParts() {
    setupAMQP()
    defer closeAMQP()

    setupDatabase()
    defer closeDatabase()

    setupPusher()

    setupMetrics()

    logParts, _ := subscribeToQueue("reporting.jobs.logs")

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

func setupMetrics() {
    Metrics = metrics.NewRegistry()

    MetricsProcessTimer = metrics.NewTimer()
    Metrics.Register("logs.process_log_part", MetricsProcessTimer)

    MetricsProcessFailedCount = metrics.NewMeter()
    Metrics.Register("logs.process_log_part.failed", MetricsProcessFailedCount)

    MetricsPusherTimer = metrics.NewTimer()
    Metrics.Register("logs.process_log_part.pusher", MetricsPusherTimer)

    MetricsPusherFailedCount = metrics.NewMeter()
    Metrics.Register("logs.process_log_part.pusher.failed", MetricsPusherFailedCount)

    go metricsLogging(60)
}

func processLogParts(logParts <-chan amqp.Delivery) {
    for part := range logParts {
        MetricsProcessTimer.Time(func() { processLogPart(part) })
    }
}

func processLogPart(part amqp.Delivery) {
    //fmt.Printf("\n\n%#v\n", string(part.Body))
    payload := parseMessageBody(part)

    logId := findLogId(payload)

    createLogPart(logId, payload)

    streamToPusher(payload)

    part.Ack(false)
}

func parseMessageBody(message amqp.Delivery) *Payload {
    var payload *Payload
    json.Unmarshal(message.Body, &payload)

    //payload.Content = strings.Replace(payload.Content, "\x00", "", -1)
    //fmt.Printf("job_id:%d number:%d\n", payload.JobId, payload.Number)
    //fmt.Printf("%#v\n", payload.Content)

    return payload
}

func findLogId(payload *Payload) string {
    var logId string
    err := JobIdFind.QueryRow(payload.JobId).Scan(&logId)

    switch {
    case err == sql.ErrNoRows:
        log.Fatalf("No log with job_id:%s", payload.JobId)
    case err != nil:
        log.Fatalf("db.queryrow: %v", err)
    }

    return logId
}

func createLogPart(logId string, payload *Payload) {
    var logPartId string
    err := LogPartsInsert.QueryRow(logId, payload.Number, payload.Content, payload.Final, time.Now()).Scan(&logPartId)

    switch {
    case err == sql.ErrNoRows:
        log.Fatalf("No new log part created")
    case err != nil:
        log.Fatalf("db.queryrow: %v", err)
    }

    // log.Println("Log Part created with id:", logPartId)
}

func streamToPusher(payload *Payload) {
    var err error

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

    MetricsPusherTimer.Time(func() {
        err = PusherClient.Publish(string(readyToPush), "job:log", fmt.Sprintf("job-%d", payload.JobId))
    })
    if err != nil {
        MetricsPusherFailedCount.Mark(1)
        log.Fatalf("pusherclient.publish: %v", err)
    }
}
