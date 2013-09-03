package main

import (
    "os"
)

func startLogPartsProcessing() {
    var err error

    logger.Println("Starting Log Stream Processing")

    if err = testDatabaseConnection(); err != nil {
        logger.Fatalf("startLogPartsProcessing: fatal error connection to the database - %v\n", err)
    }

    if _, err = newPusherClient(); err != nil {
        logger.Fatalf("startLogPartsProcessing: error setting up Pusher - %v\n", err)
    }

    startMetricsLogging(appMetrics, logger)

    logger.Println("Connecting to AMQP")

    amqp, err := NewMessageBroker(os.Getenv("RABBITMQ_URL"))
    if err != nil {
        logger.Fatalf("startLogPartsProcessing: error connecting to Rabbit - %v\n", err)
    }
    defer amqp.Close()

    logger.Printf("Subscribing to reporting.jobs.logs")

    err = amqp.Subscribe("reporting.jobs.logs", 20, createLogPartsProcessor)
    if err != nil {
        logger.Fatalf("startLogPartsProcessing: error setting up subscriptions - %v\n", err)
    }
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

func createLogPartsProcessor(logProcessorNum int) func([]byte) {
    logger.Printf("Starting Log Processor %d", logProcessorNum+1)

    db, err := NewRealDB(os.Getenv("DATABASE_URL"))
    if err != nil {
        logger.Printf("createLogPartsProcessor: [%d] fatal error connecting to the database - %v\n", logProcessorNum+1, err)
        return nil
    }

    pc, err := newPusherClient()
    if err != nil {
        logger.Printf("createLogPartsProcessor: [%d] fatal error setting up pusher - %v\n", logProcessorNum+1, err)
        return nil
    }

    lpp := LogPartsProcessor{db, pc}

    return func(message []byte) {
        var err error

        appMetrics.TimeLogPartProcessing(func() {
            err = lpp.Process(message)
        })

        if err != nil {
            logger.Printf("ERROR %v\n", err)
            appMetrics.MarkFailedLogPartCount()
        }
    }
}
