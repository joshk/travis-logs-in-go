package main

import (
    "log"
    "os"
    "sync"
)

func startLogPartsProcessing() {
    var err error

    log.Println("Starting Log Stream Processing")

    if err = testDatabaseConnection(); err != nil {
        log.Fatalf("startLogPartsProcessing: fatal error connection to the database - %v", err)
    }

    if _, err = newPusherClient(); err != nil {
        log.Fatalf("startLogPartsProcessing: error setting up Pusher - %v", err)
    }

    appMetrics.StartLogging()

    log.Println("Connecting to AMQP")

    amqp, err := NewMessageBroker(os.Getenv("RABBITMQ_URL"))
    if err != nil {
        log.Fatalf("startLogPartsProcessing: error connecting to Rabbit - %v", err)
    }
    defer amqp.Close()

    log.Printf("Subscribing to reporting.jobs.logs")

    var wg sync.WaitGroup
    wg.Add(10)
    for i := 0; i < 10; i++ {
        go func() {
            defer wg.Done()

            err = amqp.Subscribe("reporting.jobs.logs", 2, createLogPartsProcessor)
            if err != nil {
                log.Fatalf("startLogPartsProcessing: error setting up subscriptions - %v", err)
            }

        }()
    }
    wg.Wait()
}

func testDatabaseConnection() error {
    log.Println("Checking the database connection details")
    db, err := NewRealDB(os.Getenv("DATABASE_URL"))
    if err != nil {
        return err
    }
    defer db.Close()

    return nil
}

func newPusherClient() (Pusher, error) {
    p, err := NewPusher(os.Getenv("PUSHER_KEY"), os.Getenv("PUSHER_SECRET"), os.Getenv("PUSHER_APP_ID"))
    if err != nil {
        return nil, err
    }
    return p, nil
}

func createLogPartsProcessor(logProcessorNum int) MessageProcessor {
    log.Printf("Starting Log Processor %d", logProcessorNum+1)

    db, err := NewRealDB(os.Getenv("DATABASE_URL"))
    if err != nil {
        log.Printf("createLogPartsProcessor: [%d] fatal error connecting to the database - %v", logProcessorNum+1, err)
        return nil
    }

    pc, err := newPusherClient()
    if err != nil {
        log.Printf("createLogPartsProcessor: [%d] fatal error setting up pusher - %v", logProcessorNum+1, err)
        return nil
    }

    return &LogPartsProcessor{db, pc}
}
