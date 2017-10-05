package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
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
		ConsumeRetryInterval:      2 * time.Second,
		PrefetchCount:             1,
		RetryTimeoutBeforeRequeue: 60 * time.Second,
	})

	if err != nil {
		panic(err)
	}

	consumerA.Subscribe("object.eventA", func(e messaging.Event) error {
		fmt.Println("object.eventA:", string(e.Body))
		return nil
	})

	consumerA.Subscribe("object.eventB", func(e messaging.Event) error {
		fmt.Println("object.eventB:", string(e.Body))
		return nil
	})

	consumerA.SubscribeWithOptions(messaging.SubscribeOptions{
		Action: "object.eventToRetryDelay",
		Handler: func(e messaging.Event) error {
			fmt.Println("object.eventToRetryDelay:", string(e.Body))
			return fmt.Errorf("Try again.")
		},
		RetryDelay:   10 * time.Second,
		DelayedRetry: true,
		MaxRetries:   30,
	})

	consumerA.SubscribeWithOptions(messaging.SubscribeOptions{
		Action: "object.eventToRetry",
		Handler: func(e messaging.Event) error {
			fmt.Println("object.eventToRetry:", string(e.Body))
			return fmt.Errorf("Try again.")
		},
		RetryDelay:   1 * time.Second,
		DelayedRetry: false,
		MaxRetries:   10,
	})

	consumerB, err := conn.Consumer(false, "events-exchange", "events-queue-b")

	if err != nil {
		panic(err)
	}

	consumerB.Subscribe("object.eventC", func(e messaging.Event) error {
		fmt.Println("object.eventC:", string(e.Body))
		return nil
	})

	consumerB.Subscribe("object.eventD", func(e messaging.Event) error {
		fmt.Println("object.eventD:", string(e.Body))
		return nil
	})

	var wg sync.WaitGroup

	go func() {
		wg.Add(1)
		consumerA.Consume()
		wg.Done()
	}()

	go func() {
		wg.Add(1)
		consumerB.Consume()
		wg.Done()
	}()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	<-sigc
	consumerA.Close()
	consumerB.Close()

	wg.Wait()
}
