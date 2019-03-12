# goevents [![Build Status](https://travis-ci.org/eventials/goevents.svg?branch=master)](https://travis-ci.org/eventials/goevents) [![GoDoc](https://godoc.org/github.com/eventials/goevents?status.svg)](http://godoc.org/github.com/eventials/goevents) [![Go Report Card](https://goreportcard.com/badge/github.com/eventials/goevents)](https://goreportcard.com/report/github.com/eventials/goevents)

Go messaging library

## About

`goevents` allows to dispatch events between applications.

An application produces events based on actions.
Another application consume these events and maybe create new events.

*Scenario:* If an application produces an event "payment.received", another application may want to delivery the product to the buyer.

## Supported Transport

- AMQP

## How to use

**The consumer**

```go
conn, err := NewConnection("amqp://guest:guest@127.0.0.1:5672/")

if err != nil {
    panic(err)
}

defer conn.Close()

c, err := NewConsumer(conn, false, "events-exchange", "events-queue")

if err != nil {
    panic(err)
}

defer c.Close()

c.Subscribe("object.*", func(body []byte) bool {
    fmt.Println(body)
    return true
})

go c.Consume()

select{}
```

**The producer**

```go
conn, err := NewConnection("amqp://guest:guest@127.0.0.1:5672/")

if err != nil {
    panic(err)
}

defer conn.Close()

p, err := NewProducer(conn, "events-exchange", "events-queue")

if err != nil {
    panic(err)
}

defer p.Close()

err = p.Publish("object.my_action", []byte("message"))

if err != nil {
    panic(err)
}
```

## Important

When using `producer`, always close all your producers (things who call the producer.Publish) before closing the producer itself (producer.Close).
In this way, you have more garanties that your messages is delivered to RabbitMQ.
