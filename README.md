# goevents [![Build Status](https://travis-ci.org/eventials/goevents.svg?branch=master)](https://travis-ci.org/eventials/goevents) [![GoDoc](https://godoc.org/github.com/eventials/goevents?status.svg)](http://godoc.org/github.com/eventials/goevents) [![Go Report Card](https://goreportcard.com/badge/github.com/eventials/goevents)](https://goreportcard.com/report/github.com/eventials/goevents)

Go messaging library

## About

`goevents` allows to dispatch events between applications.

An application produces events based on actions.
Another application consume these events and maybe create new events.

*Scenario:* If an application produces an events "payment-received", another application may want to delivery the product to the buyer.

## Supported Transport

- AMQP

## How to use

**The consumer**

```go
conn, err := NewConnection("amqp://guest:guest@127.0.0.1:5672/", "events-queue", "events-exchange")

if err != nil {
    panic(err)
}

c, err := NewConsumer(conn, false)

if err != nil {
    panic(err)
}

c.Subscribe("my_action", func(body []byte) bool {
    fmt.Println(body)
    return true
})
```

**The producer**

```go
conn, err := NewConnection("amqp://guest:guest@127.0.0.1:5672/", "events-queue", "events-exchange")

if err != nil {
    panic(err)
}

p, err := NewProducer(conn)

if err != nil {
    panic(err)
}

err = p.Publish("my_action", []byte("message"))

if err != nil {
    panic(err)
}
```

### Actions

The action can be a full word, a wildcard (`*`) or multiple words or wildcards delimited by dots (`.`)

Look the examples below:

- The action handler `my_action` will match only `my_action` event.
- The action handler `my_action.foo` will match only `my_action.foo` event.
- The action handler `my_action.*` will match `my_action.foo`, `my_action.bar` and all `my_action.*` events.
- The action handler `my_action.foo.bar` will match only `my_action.foo.bar` event.
- The action handler `my_action.*.bar` will match `my_action.foo.bar`, `my_action.bar.bar` and all `my_action.*.bar` events.
- The action handler `*` will match all events.
