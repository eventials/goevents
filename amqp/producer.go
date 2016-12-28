package amqp

import (
	"time"

	"github.com/eventials/goevents/messaging"

	amqplib "github.com/streadway/amqp"
)

type Producer struct {
	conn *Connection

	channel *amqplib.Channel
	queue   *amqplib.Queue

	exchangeName string
	queueName    string
}

// NewProducer returns a new AMQP Producer.
func NewProducer(c messaging.Connection, exchange, queue string) (messaging.Producer, error) {
	amqpConn := c.(*Connection)

	ch, err := amqpConn.connection.Channel()

	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchange, // name
		"topic",  // type
		true,     // durable
		false,    // auto-delete
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)

	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		queue, // name
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	if err != nil {
		return nil, err
	}

	return &Producer{
		amqpConn,
		ch,
		&q,
		exchange,
		queue,
	}, nil
}

// Publish publishes an action.
func (p *Producer) Publish(action string, data []byte) error {
	msg := amqplib.Publishing{
		DeliveryMode: amqplib.Persistent,
		Timestamp:    time.Now(),
		Body:         data,
	}

	return p.channel.Publish(p.exchangeName, action, false, false, msg)
}
