package main

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"fmt"
	"github.com/eventials/goevents/amqp"
)

func main() {
	conn, err := amqp.NewConnection("amqp://guest:guest@broker:5672/")

	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup

	producerA, err := amqp.NewProducer(conn, "events-exchange")
	wg.Add(1)

	if err != nil {
		panic(err)
	}

	producerB, err := amqp.NewProducer(conn, "events-exchange")
	wg.Add(1)

	if err != nil {
		panic(err)
	}

	go func() {
		for {
			producerA.Publish("object.eventA", []byte("some data"))
			producerB.Publish("object.eventC", []byte("some data"))

			time.Sleep(1 * time.Second)
		}
	}()

	go func() {
		<-producerA.NotifyClose()
		fmt.Println("Producer closed for good")
		wg.Done()
	}()

	go func() {
		<-producerB.NotifyClose()
		fmt.Println("Producer closed for good")
		wg.Done()
	}()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	fmt.Println("Waiting CTRL+C")

	<-sigc
	fmt.Println("Closing producerA")
	producerA.Close()

	fmt.Println("Closing producerB")
	producerB.Close()

	wg.Wait()
}
