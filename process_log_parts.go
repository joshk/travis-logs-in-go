package main

import (
    "github.com/streadway/amqp"
    "log"
    "os"
    "sync"
)

func startLogPartsProcessing() {
    var err error

    amqp, logParts := subscribeToLoggingQueue()
    defer amqp.Close()

    if err = testDatabaseConnection(); err != nil {
        log.Fatalf("startLogPartsProcessing: fatal error connection to the database - %v\n", err)
    }

    if _, err = newPusherClient(); err != nil {
        log.Fatalf("startLogPartsProcessing: error setting up Pusher - %v\n", err)
    }

    metrics := NewMetrics()
    startMetricsLogging(metrics)

    var wg sync.WaitGroup
    wg.Add(20)
    for i := 0; i < 20; i++ {
        go func(logProcessorNum int) {
            defer wg.Done()

            log.Printf("Starting Log Processor %d", logProcessorNum+1)

            db, err := NewDB(os.Getenv("DATABASE_URL"))
            if err != nil {
                log.Printf("startLogPartsProcessing: [%d] fatal error connecting to the database - %v\n", logProcessorNum+1, err)
                return
            }

            pc, err := newPusherClient()
            if err != nil {
                log.Printf("startLogPartsProcessing: [%d] fatal error setting up pusher - %v\n", logProcessorNum+1, err)
                return
            }

            lpp := LogPartsProcessor{db, pc, metrics}

            processLogParts(&lpp, logParts)

            log.Printf("Log Processor %d exited", logProcessorNum+1)
        }(i)
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

func testDatabaseConnection() error {
    log.Println("Checking the database connection details")
    db, err := NewDB(os.Getenv("DATABASE_URL"))
    if err != nil {
        return err
    }
    defer db.Close()

    return nil
}

func newPusherClient() (*Pusher, error) {
    p, err := NewPusher(os.Getenv("PUSHER_KEY"), os.Getenv("PUSHER_SECRET"), os.Getenv("PUSHER_APP_ID"))
    if err != nil {
        return nil, err
    }
    return p, nil
}

func subscribeToLoggingQueue() (*MessageBroker, <-chan amqp.Delivery) {
    amqp, err := NewMessageBroker(os.Getenv("RABBITMQ_URL"))
    if err != nil {
        log.Fatalf("startLogPartsProcessing: fatal error connecting to %s - %v\n", os.Getenv("RABBITMQ_URL"), err)
    }

    logParts, err := amqp.Subscribe("reporting.jobs.logs")
    if err != nil {
        amqp.Close()
        log.Fatalf("startLogPartsProcessing: fatal error subscribing to reporting.jobs.logs - %v\n", err)
    }

    return amqp, logParts
}
