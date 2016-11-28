package events

import (
	"github.com/streadway/amqp"
	"regexp"
	"strings"
)

type eventHandler func(body []byte) bool

type handler struct {
	action  string
	handler eventHandler
	re      *regexp.Regexp
}

type Consumer struct {
	conn     *Connection
	autoAck  bool
	handlers []handler
}

func NewConsumer(c *Connection, autoAck bool) *Consumer {
	return &Consumer{
		c,
		autoAck,
		make([]handler, 0),
	}
}

func (c *Consumer) dispatch(msg amqp.Delivery) {
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

func (c *Consumer) getHandler(action string) (eventHandler, bool) {
	for _, h := range c.handlers {
		if h.re.MatchString(action) {
			return h.handler, true
		}
	}

	return nil, false
}

func (c *Consumer) Subscribe(action string, handlerFn eventHandler) error {
	// TODO: Replace # pattern too.
	pattern := strings.Replace(action, "*", "(.*)", 0)
	re, err := regexp.Compile(pattern)

	if err != nil {
		return err
	}

	for _, h := range c.handlers {
		if h.action == action {
			// return fmt.Errorf("Action '%s' already registered.", action)
		}
	}

	err = c.conn.channel.QueueBind(
		c.conn.queueName,    // queue name
		action,              // routing key
		c.conn.exchangeName, // exchange
		false,               // no-wait
		nil,                 // arguments
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

func (c *Consumer) Unsubscribe(action string) error {
	err := c.conn.channel.QueueUnbind(
		c.conn.queueName,    // queue name
		action,              // routing key
		c.conn.exchangeName, // exchange
		nil,                 // arguments
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

func (c *Consumer) Listen() error {
	msgs, err := c.conn.channel.Consume(
		c.conn.queueName, // queue
		"",               // consumer
		c.autoAck,        // auto ack
		false,            // exclusive
		false,            // no local
		false,            // no wait
		nil,              // args
	)

	if err != nil {
		return err
	}

	go func() {
		for m := range msgs {
			c.dispatch(m)
		}
	}()

	return nil
}

func (c *Consumer) ListenForever() error {
	err := c.Listen()

	if err != nil {
		return err
	}

	select {}
}
