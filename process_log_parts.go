package main

import (
    "github.com/streadway/amqp"
    "log"
    "os"
    "sync"
)

func startLogPartsProcessing() {
    amqp, err := NewMessageBroker(os.Getenv("RABBITMQ_URL"))
    if err != nil {
        log.Fatalf("startLogPartsProcessing: fatal error connecting to %s - %v\n", os.Getenv("RABBITMQ_URL"), err)
    }
    defer amqp.Close()

    logParts, err := amqp.Subscribe("reporting.jobs.logs")
    if err != nil {
        log.Fatalf("startLogPartsProcessing: fatal error subscribing to reporting.jobs.logs - %v\n", err)
    }

    metrics := NewMetrics()
    startMetricsLogging(metrics)

    var wg sync.WaitGroup
    wg.Add(20)
    for i := 0; i < 20; i++ {
        go func() {
            defer wg.Done()

            db, err := NewDB(os.Getenv("DATABASE_URL"))
            if err != nil {
                log.Printf("startLogPartsProcessing: fatal error connecting to the database - %v\n", err)
                return
            }

            pc, err := NewPusher(os.Getenv("PUSHER_KEY"), os.Getenv("PUSHER_SECRET"), os.Getenv("PUSHER_APP_ID"))
            if err != nil {
                log.Printf("startLogPartsProcessing: fatal error setting up pusher - %v\n", err)
                return
            }

            lpp := LogPartsProcessor{db, pc, metrics}

            processLogParts(&lpp, logParts)
        }()
    }
    wg.Wait()
}

func processLogParts(lpp *LogPartsProcessor, logParts <-chan amqp.Delivery) {
    for part := range logParts {
        var err error

        lpp.metrics.TimeLogPartProcessing(func() {
            err = lpp.Process(part.Body)
        })

        if err != nil {
            log.Printf("ERROR %v\n", err)
            lpp.metrics.MarkFailedLogPartCount()
        }

        part.Ack(false)
    }
}
