package main

import (
	"fmt"

	"github.com/eventials/goevents/messaging"
	"github.com/eventials/goevents/sns"
)

func main() {
	consumer := sns.NewConsumer(sns.ConsumerConfig{
		AccessKey: "",
		SecretKey: "",
		Region:    "us-east-1",
		QueueUrl:  "https://sqs.us-east-1.amazonaws.com/0000000000/vlab-exams-mp4-dev",
	})

	consumer.Subscribe("arn:aws:sns:us-east-1:0000000000:test", func(e messaging.Event) error {
		fmt.Println(e)
		return nil
	}, nil)

	consumer.Subscribe("arn:aws:sns:us-east-1:0000000000:test2", func(e messaging.Event) error {
		fmt.Println(e)
		return nil
	}, nil)

	consumer.Consume()
}
