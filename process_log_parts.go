package main

import (
    "database/sql"
    "encoding/json"
    "fmt"
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

func ProcessLogParts() {
    setupAMQP()
    defer closeAMQP()

    setupDatabase()
    defer closeDatabase()

    setupPusher()

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

func processLogParts(logParts <-chan amqp.Delivery) {
    for part := range logParts {
        //fmt.Printf("\n\n%#v\n", string(part.Body))
        var payload Payload
        json.Unmarshal(part.Body, &payload)
        //payload.Content = strings.Replace(payload.Content, "\x00", "", -1)
        fmt.Printf("job_id:%d number:%d\n", payload.JobId, payload.Number)
        //fmt.Printf("%#v\n", payload.Content)

        logId := findLogId(payload)

        createLogPart(logId, payload)

        streamToPusher(payload)

        part.Ack(false)
    }
}

func findLogId(payload Payload) string {
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

func createLogPart(logId string, payload Payload) {
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

func streamToPusher(payload Payload) {
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
