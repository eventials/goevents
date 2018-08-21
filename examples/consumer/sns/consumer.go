package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/eventials/goevents/messaging"
	"github.com/eventials/goevents/sns"
	"github.com/sirupsen/logrus"
)

func main() {
	logrus.SetLevel(logrus.DebugLevel)

	consumer := sns.MustNewConsumer(&sns.ConsumerConfig{
		AccessKey:           "",
		SecretKey:           "",
		Region:              "us-east-1",
		QueueUrl:            "https://sqs.us-east-1.amazonaws.com/0000000000/test-queue",
		MaxNumberOfMessages: 5,
	})

	defer consumer.Close()
	defer consumer.Close()

	consumer.Subscribe("arn:aws:sns:us-east-1:0000000000:test", func(e messaging.Event) error {
		fmt.Println("Action:\t", e.Action)
		fmt.Println("Body:\t", string(e.Body))

		time.Sleep(time.Minute)

		return nil
	}, nil)

	consumer.Subscribe("arn:aws:sns:us-east-1:0000000000:test2", func(e messaging.Event) error {
		fmt.Println("Action:\t", e.Action)
		fmt.Println("Body:\t", string(e.Body))
		return nil
	}, nil)

	go consumer.Consume()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	fmt.Println("Waiting CTRL+C")

	<-sigc
}
