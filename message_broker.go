package main

import (
    "github.com/streadway/amqp"
    "log"
)

type MessageBroker struct {
    conn    *amqp.Connection
    channel *amqp.Channel
}

func NewMessageBroker(url string) (*MessageBroker, error) {
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

    return &MessageBroker{conn, nil}, nil
}

func (mb *MessageBroker) Close() {
    if mb.channel != nil {
        mb.channel.Close()
    }
    mb.conn.Close()
}

func (mb *MessageBroker) Subscribe(queueName string) (<-chan amqp.Delivery, error) {
    ch, err := mb.conn.Channel()
    if err != nil {
        return nil, err
    }

    mb.channel = ch

    err = mb.channel.Qos(20, 0, false)
    if err != nil {
        return nil, err
    }

    messages, err := mb.channel.Consume(queueName, "processor", false, false, false, false, nil)
    if err != nil {
        return nil, err
    }

    return messages, nil
}
