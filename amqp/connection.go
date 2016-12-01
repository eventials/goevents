package amqp

import (
	base "github.com/eventials/goevents"
	amqplib "github.com/streadway/amqp"
)

type Connection struct {
	connection *amqplib.Connection
	channel    *amqplib.Channel
	queue      *amqplib.Queue

	exchangeName string
	queueName    string
}

func NewConnection(url, exchange, queue string) (base.Connection, error) {
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

func (c *Connection) Consumer(autoAck bool) (base.Consumer, error) {
	return NewConsumer(c, autoAck)
}

func (c *Connection) Producer() (base.Producer, error) {
	return NewProducer(c)
}

func (c *Connection) Close() {
	c.channel.Close()
	c.connection.Close()
}
