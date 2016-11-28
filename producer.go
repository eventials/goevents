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

func (p *Producer) Publish(action string, data []byte) error {
	msg := amqp.Publishing{
		DeliveryMode:    amqp.Persistent,
		Timestamp:       time.Now(),
		Body:            data,
	}

	return p.conn.channel.Publish(p.conn.exchangeName, action, false, false, msg)
}
