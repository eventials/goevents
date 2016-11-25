package main

import (
	"time"

	"github.com/streadway/amqp"
)

type Producer struct {
	conn *Connection
}

func NewProducer(c *Connection) *Producer {
	return &Producer{
		c,
	}
}

func (p *Producer) Publish(action string, data interface{}) error {
	msg := amqp.Publishing{
		DeliveryMode:    amqp.Persistent,
		Timestamp:       time.Now(),
		ContentType:     "application/json",
		ContentEncoding: "utf-8",
		Body:            data,
	}

	return c.channel.Publish(c.exchangeName, routingKey, false, false, msg)
}
