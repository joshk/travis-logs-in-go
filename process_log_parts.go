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
        var err error
        MetricsProcessTimer.Time(func() {
            err = processLogPart(part)
        })
        if err != nil {
            log.Printf("ERROR %v\n", err)
            MetricsProcessFailedCount.Mark(1)
        }
    }
}

func processLogPart(part amqp.Delivery) error {
    var err error
    //fmt.Printf("\n\n%#v\n", string(part.Body))
    payload, err := parseMessageBody(part)
    if err != nil {
        return err
    }

    logId, err := findLogId(payload)
    if err != nil {
        return err
    }

    err = createLogPart(logId, payload)
    if err != nil {
        return err
    }

    err = streamToPusher(payload)
    if err != nil {
        return err
    }

    part.Ack(false)

    return nil
}

func parseMessageBody(message amqp.Delivery) (*Payload, error) {
    var payload *Payload
    err := json.Unmarshal(message.Body, &payload)

    if err != nil {
        return nil, fmt.Errorf("[parsemessagebody] error during json.unmarshal: %v", err)
    }

    //payload.Content = strings.Replace(payload.Content, "\x00", "", -1)
    //fmt.Printf("job_id:%d number:%d\n", payload.JobId, payload.Number)
    //fmt.Printf("%#v\n", payload.Content)

    return payload, nil
}

func findLogId(payload *Payload) (string, error) {
    var logId string
    err := JobIdFind.QueryRow(payload.JobId).Scan(&logId)

    switch {
    case err == sql.ErrNoRows:
        return "", fmt.Errorf("[findlogid] No log with job_id:%s found", payload.JobId)
    case err != nil:
        return "", fmt.Errorf("[findlogid] db query failed: %v", err)
    }

    return logId, nil
}

func createLogPart(logId string, payload *Payload) error {
    var logPartId string
    err := LogPartsInsert.QueryRow(logId, payload.Number, payload.Content, payload.Final, time.Now()).Scan(&logPartId)

    switch {
    case err == sql.ErrNoRows:
        return fmt.Errorf("[createlogpart] log part number:%s for job_id:%s could not be created. (%v)", payload.Number, payload.JobId, err)
    case err != nil:
        return fmt.Errorf("[createlogpart] db query failed: %v", err)
    }

    return nil
}

func streamToPusher(payload *Payload) error {
    var err error

    pusherPayload := PusherPayload{
        JobId:   payload.JobId,
        Number:  payload.Number,
        Content: payload.Content,
        Final:   payload.Final,
    }

    readyToPush, err := json.Marshal(pusherPayload)
    if err != nil {
        return fmt.Errorf("[streamtopusher] error during json.marshal: %v", err)
    }

    MetricsPusherTimer.Time(func() {
        err = PusherClient.Publish(string(readyToPush), "job:log", fmt.Sprintf("job-%d", payload.JobId))
    })
    if err != nil {
        MetricsPusherFailedCount.Mark(1)
        return fmt.Errorf("[streamtopusher] error during publishing to pusher: %v", err)
    }

    return nil
}
