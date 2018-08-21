package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/eventials/goevents/sns"
)

func main() {
	wg := sync.WaitGroup{}

	producer := sns.MustNewProducer(sns.ProducerConfig{
		AccessKey:       "",
		SecretKey:       "",
		Region:          "us-east-1",
		PublishInterval: 2 * time.Second,
	})

	go func() {
		for {
			producer.Publish("arn:aws:sns:us-east-1:0000000000:test2", []byte("some data"))

			time.Sleep(20 * time.Second)
		}
	}()

	wg.Add(1)
	go func() {
		<-producer.NotifyClose()
		fmt.Println("Producer closed for good")
		wg.Done()
	}()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	fmt.Println("Waiting CTRL+C")

	<-sigc

	fmt.Println("Closing producer")
	producer.Close()

	wg.Wait()
}
