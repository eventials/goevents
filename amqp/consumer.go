package amqp

import (
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/eventials/goevents/messaging"

	log "github.com/Sirupsen/logrus"
	amqplib "github.com/streadway/amqp"
)

type handler struct {
	action  string
	handler messaging.EventHandler
	re      *regexp.Regexp
}

type Consumer struct {
	config ConsumerConfig
	m      sync.Mutex

	conn     *Connection
	autoAck  bool
	handlers []handler

	channel *amqplib.Channel
	queue   *amqplib.Queue

	exchangeName string
	queueName    string

	closed bool
}

// ConsumerConfig to be used when creating a new producer.
type ConsumerConfig struct {
	consumeRetryInterval time.Duration
}

// NewConsumer returns a new AMQP Consumer.
// Uses a default ConsumerConfig with 2 second of consume retry interval.
func NewConsumer(c messaging.Connection, autoAck bool, exchange, queue string) (messaging.Consumer, error) {
	return NewConsumerConfig(c, autoAck, exchange, queue, ConsumerConfig{
		consumeRetryInterval: 2 * time.Second,
	})
}

// NewConsumerConfig returns a new AMQP Consumer.
func NewConsumerConfig(c messaging.Connection, autoAck bool, exchange, queue string, config ConsumerConfig) (messaging.Consumer, error) {
	consumer := &Consumer{
		config:       config,
		conn:         c.(*Connection),
		autoAck:      autoAck,
		handlers:     make([]handler, 0),
		exchangeName: exchange,
		queueName:    queue,
	}

	err := consumer.setupTopology()

	go consumer.handleReestablishedConnnection()

	return consumer, err
}

func (c *Consumer) Close() {
	c.closed = true
	c.channel.Close()
}

func (c *Consumer) setupTopology() error {
	c.m.Lock()
	defer c.m.Unlock()

	var err error

	c.channel, err = c.conn.OpenChannel()

	if err != nil {
		return err
	}

	err = c.channel.ExchangeDeclare(
		c.exchangeName, // name
		"topic",        // type
		true,           // durable
		false,          // auto-delete
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)

	if err != nil {
		return err
	}

	q, err := c.channel.QueueDeclare(
		c.queueName, // name
		true,        // durable
		false,       // auto-delete
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)

	c.queue = &q

	if err != nil {
		return err
	}

	return nil
}

func (c *Consumer) handleReestablishedConnnection() {
	for !c.closed {
		<-c.conn.NotifyReestablish()

		err := c.setupTopology()

		if err != nil {
			log.WithFields(log.Fields{
				"type":    "goevents",
				"subType": "consumer",
				"error":   err,
			}).Error("Error setting up topology after reconnection")
		}
	}
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
func (c *Consumer) Consume() {
	for !c.closed {
		log.WithFields(log.Fields{
			"type":    "goevents",
			"subType": "consumer",
			"queue":   c.queueName,
		}).Debug("Setting up consumer channel...")

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
			log.WithFields(log.Fields{
				"type":    "goevents",
				"subType": "consumer",
				"queue":   c.queueName,
				"error":   err,
			}).Error("Error setting up consumer...")

			time.Sleep(c.config.consumeRetryInterval)

			continue
		}

		log.WithFields(log.Fields{
			"type":    "goevents",
			"subType": "consumer",
			"queue":   c.queueName,
		}).Info("Consuming messages...")

		for m := range msgs {
			c.dispatch(m)
		}

		log.WithFields(log.Fields{
			"type":    "goevents",
			"subType": "consumer",
			"queue":   c.queueName,
			"closed":  c.closed,
		}).Info("Consumption finished")
	}
}
