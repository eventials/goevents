package main

import (
	"os"
	"os/signal"
	"sync"
	"syscall"

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
