package amqp

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/eventials/goevents/messaging"

	log "github.com/sirupsen/logrus"
	amqplib "github.com/streadway/amqp"
)

var (
	logger = log.WithFields(log.Fields{
		"type":     "goevents",
		"sub_type": "consumer",
	})
)

type handler struct {
	action       string
	fn           messaging.EventHandler
	re           *regexp.Regexp
	maxRetries   int32
	retryDelay   time.Duration
	delayedRetry bool
}

type Consumer struct {
	config ConsumerConfig

	m sync.Mutex

	conn     *Connection
	autoAck  bool
	handlers []handler

	channel    *amqplib.Channel
	queue      *amqplib.Queue
	retryQueue *amqplib.Queue

	exchangeName string
	queueName    string

	closed bool
}

// ConsumerConfig to be used when creating a new producer.
type ConsumerConfig struct {
	ConsumeRetryInterval      time.Duration
	PrefetchCount             int
	RetryTimeoutBeforeRequeue time.Duration
}

// NewConsumer returns a new AMQP Consumer.
// Uses a default ConsumerConfig with 2 second of consume retry interval.
func NewConsumer(c messaging.Connection, autoAck bool, exchange, queue string) (messaging.Consumer, error) {
	return NewConsumerConfig(c, autoAck, exchange, queue, ConsumerConfig{
		ConsumeRetryInterval:      2 * time.Second,
		PrefetchCount:             0,
		RetryTimeoutBeforeRequeue: 30 * time.Second,
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

	err = c.channel.Qos(c.config.PrefetchCount, 0, true)

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

	if err != nil {
		return err
	}

	c.queue = &q

	return nil
}

func (c *Consumer) handleReestablishedConnnection() {
	for !c.closed {
		<-c.conn.NotifyReestablish()

		err := c.setupTopology()

		if err != nil {
			logger.WithFields(log.Fields{
				"error": err,
			}).Error("Error setting up topology after reconnection.")
		}
	}
}

func (c *Consumer) dispatch(msg amqplib.Delivery) {
	if h, ok := c.getHandler(msg); ok {
		delay, isRetry := getXRetryDelayHeader(msg)

		if !isRetry {
			delay = h.retryDelay
		}

		retryCount, _ := getXRetryCountHeader(msg)

		defer func() {
			if err := recover(); err != nil {
				if h.maxRetries > 0 {
					c.retryMessage(msg, h, retryCount, delay)
				} else {
					logger.WithFields(log.Fields{
						"error":      err,
						"message_id": msg.MessageId,
					}).Error("Failed to process event.")

					if !c.autoAck {
						msg.Ack(false)
					}
				}
			}
		}()

		death, isRetry := getXRetryDeathHeader(msg)

		if isRetry {
			since := time.Since(death)
			tts := delay - since

			logger.WithFields(log.Fields{
				"max_retries":     h.maxRetries,
				"message_id":      msg.MessageId,
				"retry_in_millis": tts,
				"retry_timeout":   c.config.RetryTimeoutBeforeRequeue,
			}).Info("Retrying message.")

			if since < delay {
				select {
				case <-time.After(c.config.RetryTimeoutBeforeRequeue):
					logger.WithFields(log.Fields{
						"max_retries": h.maxRetries,
						"message_id":  msg.MessageId,
					}).Info("Requeue message.")

					c.requeueMessage(msg)
				case <-time.After(tts):
					logger.WithFields(log.Fields{
						"max_retries": h.maxRetries,
						"message_id":  msg.MessageId,
					}).Info("Dispathing retry message.")

					c.doDispatch(msg, h, retryCount, delay)
				}

				return
			}
		}

		c.doDispatch(msg, h, retryCount, delay)
	} else {
		// got wrong message?
		// ignore and don't requeue.
		if !c.autoAck {
			msg.Nack(false, false)
		}
	}
}

func (c *Consumer) doDispatch(msg amqplib.Delivery, h *handler, retryCount int32, delay time.Duration) {
	err := h.fn(messaging.Event{
		Action:  h.action,
		Body:    msg.Body,
		Context: context.Background(),
	})

	if err != nil {
		if h.maxRetries > 0 {
			if retryCount >= h.maxRetries {
				logger.WithFields(log.Fields{
					"max_retries": h.maxRetries,
					"message_id":  msg.MessageId,
				}).Error("Maximum retries reached. Giving up.")

				if !c.autoAck {
					msg.Ack(false)
				}
			} else {
				logger.WithFields(log.Fields{
					"error":      err,
					"message_id": msg.MessageId,
				}).Error("Failed to process event. Retrying...")

				c.retryMessage(msg, h, retryCount, delay)
			}
		} else {
			logger.WithFields(log.Fields{
				"error":      err,
				"message_id": msg.MessageId,
			}).Error("Failed to process event.")
		}
	} else if !c.autoAck {
		msg.Ack(false)
	}
}

func (c *Consumer) retryMessage(msg amqplib.Delivery, h *handler, retryCount int32, delay time.Duration) {
	delayNs := delay.Nanoseconds()

	if h.delayedRetry {
		delayNs *= 2
	}

	retryMsg := amqplib.Publishing{
		Headers: amqplib.Table{
			"x-retry-death": time.Now().UTC(),
			"x-retry-count": retryCount + 1,
			"x-retry-max":   h.maxRetries,
			"x-retry-delay": delayNs,
			"x-action-key":  getAction(msg),
		},
		Timestamp:    time.Now(),
		DeliveryMode: msg.DeliveryMode,
		Body:         msg.Body,
		MessageId:    msg.MessageId,
	}

	err := c.channel.Publish("", c.queueName, false, false, retryMsg)

	if err != nil {
		logger.WithFields(log.Fields{
			"error": err,
		}).Error("Failed to retry.")

		if !c.autoAck {
			msg.Nack(false, true)
		}
	} else if !c.autoAck {
		msg.Ack(false)
	}
}

func (c *Consumer) requeueMessage(msg amqplib.Delivery) {
	retryMsg := amqplib.Publishing{
		Headers:      msg.Headers,
		Timestamp:    msg.Timestamp,
		DeliveryMode: msg.DeliveryMode,
		Body:         msg.Body,
		MessageId:    msg.MessageId,
	}

	err := c.channel.Publish("", c.queueName, false, false, retryMsg)

	if err != nil {
		logger.WithFields(log.Fields{
			"messageId": msg.MessageId,
			"error":     err,
		}).Error("Failed to requeue.")

		if !c.autoAck {
			msg.Nack(false, true)
		}
	} else if !c.autoAck {
		msg.Ack(false)
	}
}

func (c *Consumer) getHandler(msg amqplib.Delivery) (*handler, bool) {
	action := getAction(msg)

	for _, h := range c.handlers {
		if h.re.MatchString(action) {
			return &h, true
		}
	}

	return nil, false
}

// Subscribe allows to subscribe an action handler.
func (c *Consumer) Subscribe(action string, handlerFn messaging.EventHandler, options *messaging.SubscribeOptions) error {
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

	if options == nil {
		options = &messaging.SubscribeOptions{
			RetryDelay:   time.Duration(0),
			DelayedRetry: false,
			MaxRetries:   0,
		}
	}

	c.handlers = append(c.handlers, handler{
		action:       action,
		fn:           handlerFn,
		re:           re,
		maxRetries:   options.MaxRetries,
		retryDelay:   options.RetryDelay,
		delayedRetry: options.DelayedRetry,
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
	logger.Info("Registered handlers:")

	for _, handler := range c.handlers {
		logger.Infof("  %s", handler.action)
	}

	for !c.closed {
		logger.WithFields(log.Fields{
			"queue": c.queueName,
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
			logger.WithFields(log.Fields{
				"queue": c.queueName,
				"error": err,
			}).Error("Error setting up consumer...")

			time.Sleep(c.config.ConsumeRetryInterval)

			continue
		}

		logger.WithFields(log.Fields{
			"queue": c.queueName,
		}).Info("Consuming messages...")

		for m := range msgs {
			go c.dispatch(m)
		}

		logger.WithFields(log.Fields{
			"queue":  c.queueName,
			"closed": c.closed,
		}).Info("Consumption finished.")
	}
}

func getAction(msg amqplib.Delivery) string {
	if ac, ok := msg.Headers["x-action-key"]; ok {
		return ac.(string)
	} else {
		return msg.RoutingKey
	}
}

func getXRetryDeathHeader(msg amqplib.Delivery) (time.Time, bool) {
	if d, ok := msg.Headers["x-retry-death"]; ok {
		return d.(time.Time), true
	}

	return time.Time{}, false
}

func getXRetryCountHeader(msg amqplib.Delivery) (int32, bool) {
	if c, ok := msg.Headers["x-retry-count"]; ok {
		return c.(int32), true
	}

	return 0, false
}

func getXRetryDelayHeader(msg amqplib.Delivery) (time.Duration, bool) {
	if d, ok := msg.Headers["x-retry-delay"]; ok {
		f, ok := d.(int64)

		if ok {
			return time.Duration(f), true
		}
	}

	return time.Duration(0), false
}
