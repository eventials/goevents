// Package events implements a messaging library
//
// About
//
// goevents allows to dispatch events between applications.
//
// An application produces events based on actions.
// Another application consume these events and maybe create new events.
//
// Supported Transport
//
// AMQP
//
// How to use
//
// The consumer
//
//    conn, err := NewConnection("amqp://guest:guest@127.0.0.1:5672/", "events-queue", "events-exchange")
//
//    if err != nil {
//        panic(err)
//    }
//
//    c, err := NewConsumer(conn, false)
//
//    if err != nil {
//        panic(err)
//    }
//
//    c.Subscribe("my_action", func(body []byte) bool {
//        fmt.Println(body)
//        return true
//    })
//
// The producer
//
//    conn, err := NewConnection("amqp://guest:guest@127.0.0.1:5672/", "events-queue", "events-exchange")
//
//    if err != nil {
//        panic(err)
//    }
//
//    p, err := NewProducer(conn)
//
//    if err != nil {
//        panic(err)
//    }
//
//    err = p.Publish("my_action", []byte("message"))
//
//    if err != nil {
//        panic(err)
//    }
//
package events
