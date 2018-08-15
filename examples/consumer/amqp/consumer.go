package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/eventials/goevents/amqp"
	"github.com/eventials/goevents/messaging"
)

func main() {
	conn, err := amqp.NewConnection("amqp://guest:guest@broker:5672/")

	if err != nil {
		panic(err)
	}

	consumerA, err := amqp.NewConsumerConfig(conn, false, "events-exchange", "events-queue-a", amqp.ConsumerConfig{
		ConsumeRetryInterval: 2 * time.Second,
		PrefetchCount:        1,
	})

	if err != nil {
		panic(err)
	}

	defer consumerA.Close()

	consumerA.Subscribe("object.eventA", func(e messaging.Event) error {
		fmt.Println("object.eventA:", string(e.Body))
		return nil
	}, nil)

	consumerA.Subscribe("object.eventB", func(e messaging.Event) error {
		fmt.Println("object.eventB:", string(e.Body))
		return nil
	}, nil)

	consumerA.Subscribe("object.eventToRetryDelay", func(e messaging.Event) error {
		fmt.Println("object.eventToRetryDelay:", string(e.Body))
		return fmt.Errorf("Try again.")
	}, &messaging.SubscribeOptions{
		RetryDelay:   10 * time.Second,
		DelayedRetry: true,
		MaxRetries:   30,
	})

	consumerA.Subscribe("object.eventToRetry", func(e messaging.Event) error {
		fmt.Println("object.eventToRetry:", string(e.Body))
		return fmt.Errorf("Try again.")
	}, &messaging.SubscribeOptions{
		RetryDelay:   1 * time.Second,
		DelayedRetry: false,
		MaxRetries:   10,
	})

	go consumerA.Consume()

	consumerB, err := conn.Consumer(false, "events-exchange", "events-queue-b")

	if err != nil {
		panic(err)
	}

	defer consumerB.Close()

	consumerB.Subscribe("object.eventC", func(e messaging.Event) error {
		fmt.Println("object.eventC:", string(e.Body))
		return nil
	}, nil)

	consumerB.Subscribe("object.eventD", func(e messaging.Event) error {
		fmt.Println("object.eventD:", string(e.Body))
		return nil
	}, nil)

	go consumerB.Consume()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	<-sigc
}
