package amqp

import (
	"time"

	"github.com/eventials/goevents/messaging"

	amqplib "github.com/streadway/amqp"
)

type Producer struct {
	conn *Connection
}

// NewProducer returns a new AMQP Producer.
func NewProducer(c messaging.Connection) (messaging.Producer, error) {
	amqpConn := c.(*Connection)

	return &Producer{
		amqpConn,
	}, nil
}

// Publish publishes an action.
func (p *Producer) Publish(action string, data []byte) error {
	msg := amqplib.Publishing{
		DeliveryMode: amqplib.Persistent,
		Timestamp:    time.Now(),
		Body:         data,
	}

	return p.conn.channel.Publish(p.conn.exchangeName, action, false, false, msg)
}
