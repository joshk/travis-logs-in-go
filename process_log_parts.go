package main

import (
    "os"
    "sync"
)

func startLogPartsProcessing() {
    var err error

    logger.Println("Starting Log Stream Processing")

    if err = testDatabaseConnection(); err != nil {
        logger.Fatalf("startLogPartsProcessing: fatal error connection to the database - %v", err)
    }

    if _, err = newPusherClient(); err != nil {
        logger.Fatalf("startLogPartsProcessing: error setting up Pusher - %v", err)
    }

    appMetrics.StartLogging(logger)

    logger.Println("Connecting to AMQP")

    amqp, err := NewMessageBroker(os.Getenv("RABBITMQ_URL"))
    if err != nil {
        logger.Fatalf("startLogPartsProcessing: error connecting to Rabbit - %v", err)
    }
    defer amqp.Close()

    logger.Printf("Subscribing to reporting.jobs.logs")

    var wg sync.WaitGroup
    wg.Add(3)
    for i := 0; i < 3; i++ {
        go func() {
            defer wg.Done()

            err = amqp.Subscribe("reporting.jobs.logs", 10, createLogPartsProcessor)
            if err != nil {
                logger.Fatalf("startLogPartsProcessing: error setting up subscriptions - %v", err)
            }

        }()
    }
    wg.Wait()
}

func testDatabaseConnection() error {
    logger.Println("Checking the database connection details")
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
    logger.Printf("Starting Log Processor %d", logProcessorNum+1)

    db, err := NewRealDB(os.Getenv("DATABASE_URL"))
    if err != nil {
        logger.Printf("createLogPartsProcessor: [%d] fatal error connecting to the database - %v", logProcessorNum+1, err)
        return nil
    }

    pc, err := newPusherClient()
    if err != nil {
        logger.Printf("createLogPartsProcessor: [%d] fatal error setting up pusher - %v", logProcessorNum+1, err)
        return nil
    }

    return &LogPartsProcessor{db, pc}
}
