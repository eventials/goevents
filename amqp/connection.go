package amqp

import (
	"github.com/eventials/goevents/messaging"

	amqplib "github.com/streadway/amqp"
)

type Connection struct {
	connection *amqplib.Connection
	channel    *amqplib.Channel
	queue      *amqplib.Queue

	exchangeName string
	queueName    string
}

// NewConnection returns an AMQP Connection.
func NewConnection(url, exchange, queue string) (messaging.Connection, error) {
	conn, err := amqplib.Dial(url)

	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()

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

	return &Connection{
		conn,
		ch,
		&q,
		exchange,
		queue,
	}, nil
}

// Consumer returns an AMQP Consumer.
func (c *Connection) Consumer(autoAck bool) (messaging.Consumer, error) {
	return NewConsumer(c, autoAck)
}

// Producer returns an AMQP Producer.
func (c *Connection) Producer() (messaging.Producer, error) {
	return NewProducer(c)
}

// Close closes the AMQP connection.
func (c *Connection) Close() {
	c.channel.Close()
	c.connection.Close()
}
