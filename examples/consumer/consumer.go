package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

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

	consumerA.Subscribe("object.eventA", func(body []byte) error {
		fmt.Println("object.eventA:", string(body))
		return nil
	})

	consumerA.Subscribe("object.eventB", func(body []byte) error {
		fmt.Println("object.eventB:", string(body))
		return nil
	})

	consumerA.SubscribeWithOptions("object.eventToRetryDelay", func(body []byte) error {
		fmt.Println("object.eventToRetryDelay:", string(body))
		return fmt.Errorf("Try again.")
	}, 1*time.Second, true, 5)

	consumerA.SubscribeWithOptions("object.eventToRetry", func(body []byte) error {
		fmt.Println("object.eventToRetry:", string(body))
		return fmt.Errorf("Try again.")
	}, 1*time.Second, false, 10)

	consumerB, err := conn.Consumer(false, "events-exchange", "events-queue-b")

	if err != nil {
		panic(err)
	}

	consumerB.Subscribe("object.eventC", func(body []byte) error {
		fmt.Println("object.eventC:", string(body))
		return nil
	})

	consumerB.Subscribe("object.eventD", func(body []byte) error {
		fmt.Println("object.eventD:", string(body))
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
