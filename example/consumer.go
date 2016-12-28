package main

import (
	"fmt"
	"github.com/eventials/goevents/amqp"
)

func main() {
	conn, err := amqp.NewConnection("amqp://guest:guest@broker:5672/")

	if err != nil {
		panic(err)
	}

	consumerA, err := conn.Consumer(false, "events-exchange", "events-queue-a")

	if err != nil {
		panic(err)
	}

	consumerA.Subscribe("object.eventA", func(body []byte) bool {
		fmt.Println("object.eventA:", string(body))
		return true
	})

	consumerA.Subscribe("object.eventB", func(body []byte) bool {
		fmt.Println("object.eventB:", string(body))
		return true
	})

	consumerB, err := conn.Consumer(false, "events-exchange", "events-queue-b")

	if err != nil {
		panic(err)
	}

	consumerB.Subscribe("object.eventC", func(body []byte) bool {
		fmt.Println("object.eventC:", string(body))
		return true
	})

	consumerB.Subscribe("object.eventD", func(body []byte) bool {
		fmt.Println("object.eventD:", string(body))
		return true
	})

	go consumerA.Consume()
	go consumerB.Consume()

	fmt.Println("Waiting messages")
	conn.WaitUntilConnectionCloses()
}
