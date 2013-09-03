package main

import (
    "github.com/streadway/amqp"
    "log"
    "sync"
)

type MessageBroker interface {
    Subscribe(string, int, func(int) func([]byte)) error
    Close()
}

type RabbitMessageBroker struct {
    conn    *amqp.Connection
    channel *amqp.Channel
}

// Force the compiler to check that RabbitMessageBroker implements MessageBroker.
var _ MessageBroker = &RabbitMessageBroker{}

func (mb *RabbitMessageBroker) Subscribe(queueName string, subCount int, f func(int) func([]byte)) error {
    ch, err := mb.conn.Channel()
    if err != nil {
        return err
    }

    err = ch.Qos(subCount, 0, false)
    if err != nil {
        return err
    }

    messages, err := ch.Consume(queueName, "processor", false, false, false, false, nil)
    if err != nil {
        return err
    }

    mb.channel = ch

    var wg sync.WaitGroup
    wg.Add(subCount)
    for i := 0; i < subCount; i++ {
        go func(logProcessorNum int) {
            defer wg.Done()

            subscription := f(logProcessorNum)

            for message := range messages {
                subscription(message.Body)
                message.Ack(false)
            }
        }(i)
    }
    wg.Wait()

    return nil
}

func (mb *RabbitMessageBroker) Close() {
    if mb.channel != nil {
        mb.channel.Close()
    }
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

    return &RabbitMessageBroker{conn, nil}, nil
}
