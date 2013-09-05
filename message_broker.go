package main

import (
    "github.com/streadway/amqp"
    "log"
    "sync"
)

type MessageBroker interface {
    Subscribe(string, int, func(int) MessageProcessor) error
    Close()
}

type MessageProcessor interface {
    Process(message []byte) error
}

type RabbitMessageBroker struct {
    conn *amqp.Connection
}

func (mb *RabbitMessageBroker) Subscribe(queueName string, subCount int, f func(int) MessageProcessor) error {
    ch, err := mb.conn.Channel()
    if err != nil {
        return err
    }
    defer ch.Close()

    err = ch.Qos(subCount*3, 0, false)
    if err != nil {
        return err
    }

    messages, err := ch.Consume(queueName, "processor", false, false, false, false, nil)
    if err != nil {
        return err
    }

    var wg sync.WaitGroup
    wg.Add(subCount)
    for i := 0; i < subCount; i++ {
        go func(logProcessorNum int) {
            defer wg.Done()

            processor := f(logProcessorNum)

            for message := range messages {
                processor.Process(message.Body)
                message.Ack(false)
            }
        }(i)
    }
    wg.Wait()

    return nil
}

func (mb *RabbitMessageBroker) Close() {
    mb.conn.Close()
}

func NewMessageBroker(url string) (MessageBroker, error) {
    var err error

    if url == "" {
        log.Fatal("We Haz No AMQP Deets")
    }

    conn, err := amqp.Dial(url)
    if err != nil {
        return nil, err
    }

    ch, err := conn.Channel()
    if err != nil {
        return nil, err
    }
    defer ch.Close()

    if _, err := ch.QueueDeclare("reporting.jobs.logs", true, false, false, false, nil); err != nil {
        return nil, err
    }

    if err = ch.ExchangeDeclare("reporting", "topic", true, false, false, false, nil); err != nil {
        return nil, err
    }

    return &RabbitMessageBroker{conn}, nil
}
