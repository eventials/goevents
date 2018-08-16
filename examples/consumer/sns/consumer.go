package main

import (
	"fmt"
	"time"

	"github.com/eventials/goevents/messaging"
	"github.com/eventials/goevents/sns"
	"github.com/sirupsen/logrus"
)

func main() {
	logrus.SetLevel(logrus.DebugLevel)

	consumer := sns.MustNewConsumer(&sns.ConsumerConfig{
		AccessKey:           "",
		SecretKey:           "0",
		Region:              "us-east-1",
		QueueUrl:            "https://sqs.us-east-1.amazonaws.com/0000000000/vlab-exams-mp4-dev",
		MaxNumberOfMessages: 5,
	})

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

	consumer.Consume()
}
