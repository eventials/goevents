package amqp

import (
	"regexp"
	"strings"

	"github.com/eventials/goevents/messaging"

	amqplib "github.com/streadway/amqp"
)

type handler struct {
	action  string
	handler messaging.EventHandler
	re      *regexp.Regexp
}

type Consumer struct {
	conn     *Connection
	autoAck  bool
	handlers []handler

	channel *amqplib.Channel
	queue   *amqplib.Queue

	exchangeName string
	queueName    string
}

// NewConsumer returns a new AMQP Consumer.
func NewConsumer(c messaging.Connection, autoAck bool, exchange, queue string) (messaging.Consumer, error) {
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

	return &Consumer{
		amqpConn,
		autoAck,
		make([]handler, 0),
		ch,
		&q,
		exchange,
		queue,
	}, nil
}

func (c *Consumer) Close() {
	c.channel.Close()
}

func (c *Consumer) dispatch(msg amqplib.Delivery) {
	if fn, ok := c.getHandler(msg.RoutingKey); ok {
		defer func() {
			if err := recover(); err != nil {
				if !c.autoAck {
					msg.Nack(false, true)
				}
			}
		}()

		ok := fn(msg.Body)

		if !c.autoAck {
			if ok {
				msg.Ack(false)
			} else {
				msg.Nack(false, true)
			}
		}
	} else {
		// got a message from wrong exchange?
		// ignore and don't requeue.
		msg.Nack(false, false)
	}
}

func (c *Consumer) getHandler(action string) (messaging.EventHandler, bool) {
	for _, h := range c.handlers {
		if h.re.MatchString(action) {
			return h.handler, true
		}
	}

	return nil, false
}

// Subscribe allow to subscribe an action handler.
func (c *Consumer) Subscribe(action string, handlerFn messaging.EventHandler) error {
	// TODO: Replace # pattern too.
	pattern := strings.Replace(action, "*", "(.*)", 0)
	re, err := regexp.Compile(pattern)

	if err != nil {
		return err
	}

	err = c.channel.QueueBind(
		c.queueName,    // queue name
		action,         // routing key
		c.exchangeName, // exchange
		false,          // no-wait
		nil,            // arguments
	)

	if err != nil {
		return err
	}

	c.handlers = append(c.handlers, handler{
		action,
		handlerFn,
		re,
	})

	return nil
}

// Unsubscribe allows to unsubscribe an action handler.
func (c *Consumer) Unsubscribe(action string) error {
	err := c.channel.QueueUnbind(
		c.queueName,    // queue name
		action,         // routing key
		c.exchangeName, // exchange
		nil,            // arguments
	)

	if err != nil {
		return err
	}

	idx := -1

	for i, h := range c.handlers {
		if h.action == action {
			idx = i
			break
		}
	}

	if idx != -1 {
		c.handlers = append(c.handlers[:idx], c.handlers[idx+1:]...)
	}

	return nil
}

// Listen start to listen for new messages.
func (c *Consumer) Consume() error {
	msgs, err := c.channel.Consume(
		c.queueName, // queue
		"",          // consumer
		c.autoAck,   // auto ack
		false,       // exclusive
		false,       // no local
		false,       // no wait
		nil,         // args
	)

	if err != nil {
		return err
	}

	for m := range msgs {
		c.dispatch(m)
	}

	return nil
}
