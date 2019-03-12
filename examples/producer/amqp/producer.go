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

	var wg sync.WaitGroup

	producerA, err := amqp.NewProducer(conn, "events-exchange")
	wg.Add(1)

	if err != nil {
		panic(err)
	}

	producerB, err := amqp.NewProducer(conn, "events-exchange")

	if err != nil {
		panic(err)
	}

	go func() {
		for {
			select {
			case <-producerA.NotifyClose():
				fmt.Println("ProducerA closed for good")
				return
			default:
				producerA.Publish("object.eventA", []byte("some data"))
			}
		}
	}()

	go func() {
		for {
			select {
			case <-producerB.NotifyClose():
				fmt.Println("ProducerB closed for good")
				return
			default:
				producerB.Publish("object.eventC", []byte("some data"))
			}
		}
	}()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	fmt.Println("Waiting CTRL+C")

	<-sigc
	fmt.Println("Closing producerA")
	producerA.Close()

	fmt.Println("Closing producerB")
	producerB.Close()
}
