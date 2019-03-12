package amqp

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
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

type consumer struct {
	config ConsumerConfig

	m  sync.RWMutex
	wg sync.WaitGroup

	conn     *connection
	autoAck  bool
	handlers []handler

	exchangeName string
	queueName    string
	closed       bool
}

var consumerTagSeq uint64

// ConsumerConfig to be used when creating a new producer.
type ConsumerConfig struct {
	ConsumeRetryInterval time.Duration
	PrefetchCount        int
	PrefixName           string
}

// NewConsumer returns a new AMQP Consumer.
// Uses a default ConsumerConfig with 2 second of consume retry interval.
func NewConsumer(c messaging.Connection, autoAck bool, exchange, queue string) (*consumer, error) {
	return NewConsumerConfig(c, autoAck, exchange, queue, ConsumerConfig{
		ConsumeRetryInterval: 2 * time.Second,
		PrefetchCount:        0,
	})
}

func createUniqueConsumerTagName() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	return fmt.Sprintf("ctag-%s-%s-%d", hostname, os.Args[0], atomic.AddUint64(&consumerTagSeq, 1))
}

// NewConsumerConfig returns a new AMQP Consumer.
func NewConsumerConfig(c messaging.Connection, autoAck bool, exchange, queue string, config ConsumerConfig) (*consumer, error) {
	consumer := &consumer{
		config:       config,
		conn:         c.(*connection),
		autoAck:      autoAck,
		handlers:     make([]handler, 0),
		exchangeName: exchange,
		queueName:    queue,
	}

	return consumer, nil

}

func (c *consumer) Close() {
	func() {
		c.m.Lock()
		defer c.m.Unlock()

		// Unsubscribe all handlers
		c.handlers = make([]handler, 0)

		c.closed = true
	}()

	// Wait all go routine finish.
	c.wg.Wait()
}

func (c *consumer) uniqueNameWithPrefix() string {
	return fmt.Sprintf("%s%d", c.config.PrefixName, time.Now().UnixNano())
}

func (c *consumer) dispatch(msg amqplib.Delivery) {
	if h, ok := c.getHandler(msg); ok {
		delay, isRetry := getXRetryDelayHeader(msg)

		if isRetry {
			logger.WithFields(log.Fields{
				"delay":      delay.String(),
				"message_id": msg.MessageId,
			}).Debug("Delaying message.")

			time.Sleep(delay)
		}

		retryCount, _ := getXRetryCountHeader(msg)

		c.doDispatch(msg, h, retryCount, delay)
	} else {
		if !c.autoAck {
			err := msg.Nack(false, true)

			if err != nil {
				logger.WithFields(log.Fields{
					"error":      err,
					"message_id": msg.MessageId,
				}).Error("Failed to nack message.")
			}
		}
	}
}

func (c *consumer) callAndHandlePanic(msg amqplib.Delivery, h *handler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			debug.PrintStack()

			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("Unknown panic")
			}
		}
	}()

	err = h.fn(messaging.Event{
		Id:        msg.MessageId,
		Action:    h.action,
		Body:      msg.Body,
		Timestamp: msg.Timestamp,
	})

	return
}

func (c *consumer) doDispatch(msg amqplib.Delivery, h *handler, retryCount int32, delay time.Duration) {
	err := c.callAndHandlePanic(msg, h)

	if err == nil {
		logger.WithFields(log.Fields{
			"action":     h.action,
			"body":       string(msg.Body),
			"message_id": msg.MessageId,
		}).Debug("Message handled successfully.")

		if !c.autoAck {
			msg.Ack(false)
		}
	} else {
		if h.maxRetries > 0 {
			if retryCount >= h.maxRetries {
				logger.WithFields(log.Fields{
					"action":     h.action,
					"body":       string(msg.Body),
					"error":      err,
					"message_id": msg.MessageId,
				}).Error("Maximum retries reached. Giving up.")

				if !c.autoAck {
					msg.Ack(false)
				}
			} else {
				logger.WithFields(log.Fields{
					"action":     h.action,
					"body":       string(msg.Body),
					"error":      err,
					"message_id": msg.MessageId,
				}).Error("Failed to process event. Retrying...")

				c.retryMessage(msg, h, retryCount, delay)
			}
		} else {
			logger.WithFields(log.Fields{
				"action":     h.action,
				"body":       string(msg.Body),
				"error":      err,
				"message_id": msg.MessageId,
			}).Error("Failed to process event.")
		}
	}
}

func (c *consumer) publishMessage(msg amqplib.Publishing, queue string) error {
	channel, err := c.conn.openChannel()

	if err != nil {
		return err
	}

	defer channel.Close()

	if err := channel.Confirm(false); err != nil {
		return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}

	confirms := channel.NotifyPublish(make(chan amqplib.Confirmation, 1))

	err = channel.Publish("", queue, false, false, msg)

	if err != nil {
		return err
	} else {
		if confirmed := <-confirms; !confirmed.Ack {
			return ErrNotAcked
		}
	}

	return nil
}

func (c *consumer) retryMessage(msg amqplib.Delivery, h *handler, retryCount int32, delay time.Duration) {
	if delay > 0 {
		if h.delayedRetry {
			delay *= 2
		}
	} else {
		delay = h.retryDelay
	}

	retryMsg := amqplib.Publishing{
		Headers: amqplib.Table{
			"x-retry-count": retryCount + 1,
			"x-retry-max":   h.maxRetries,
			"x-retry-delay": delay.String(),
			"x-action-key":  getAction(msg),
		},
		Timestamp:    msg.Timestamp,
		DeliveryMode: msg.DeliveryMode,
		Body:         msg.Body,
		MessageId:    msg.MessageId,
	}

	err := c.publishMessage(retryMsg, c.queueName)

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

func (c *consumer) getHandler(msg amqplib.Delivery) (*handler, bool) {
	c.m.RLock()
	defer c.m.RUnlock()

	action := getAction(msg)

	for _, h := range c.handlers {
		if h.re.MatchString(action) {
			return &h, true
		}
	}

	return nil, false
}

// Subscribe allows to subscribe an action handler.
func (c *consumer) Subscribe(action string, handlerFn messaging.EventHandler, options *messaging.SubscribeOptions) error {
	// TODO: Replace # pattern too.
	pattern := strings.Replace(action, "*", "(.*)", 0)
	re, err := regexp.Compile(pattern)

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

	c.m.Lock()
	defer c.m.Unlock()

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
func (c *consumer) Unsubscribe(action string) error {
	idx := -1

	c.m.Lock()
	defer c.m.Unlock()

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

func (c *consumer) BindActions(actions ...string) error {
	channel, err := c.conn.openChannel()

	if err != nil {
		return err
	}

	defer channel.Close()

	for _, action := range actions {
		err := c.bindActionToQueue(channel, c.queueName, action)

		if err != nil {
			return err
		}
	}

	return nil
}

func (c *consumer) UnbindActions(actions ...string) error {
	channel, err := c.conn.openChannel()

	if err != nil {
		return err
	}

	defer channel.Close()

	for _, action := range actions {
		err := channel.QueueUnbind(
			c.queueName,    // queue name
			action,         // routing key
			c.exchangeName, // exchange
			nil,            // arguments
		)

		if err != nil {
			return err
		}
	}

	return nil
}

func (c *consumer) bindActionToQueue(channel *amqplib.Channel, queueName string, action string) error {
	return channel.QueueBind(
		queueName,      // queue name
		action,         // routing key
		c.exchangeName, // exchange
		false,          // no-wait
		nil,            // arguments
	)
}

func (c *consumer) bindAllActionsQueue(channel *amqplib.Channel, queueName string) error {
	for _, h := range c.handlers {
		err := c.bindActionToQueue(channel, queueName, h.action)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *consumer) setupTopology(channel *amqplib.Channel) (err error) {
	err = channel.Qos(c.config.PrefetchCount, 0, true)

	if err != nil {
		return err
	}

	err = channel.ExchangeDeclare(
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

	durable := true
	exclusive := false

	if c.config.PrefixName != "" {
		c.queueName = c.uniqueNameWithPrefix()
		durable = false
		exclusive = true
	}

	_, err = channel.QueueDeclare(
		c.queueName, // name
		durable,     // durable
		false,       // auto-delete
		exclusive,   // exclusive
		false,       // no-wait
		nil,         // arguments
	)

	if err != nil {
		return err
	}

	return c.bindAllActionsQueue(channel, c.queueName)
}

func (c *consumer) doConsume() error {
	logger.WithFields(log.Fields{
		"queue": c.queueName,
	}).Debug("Setting up consumer channel...")

	channel, err := c.conn.openChannel()

	if err != nil {
		return err
	}

	defer channel.Close()

	err = c.setupTopology(channel)

	if err != nil {
		return err
	}

	msgs, err := channel.Consume(
		c.queueName,                   // queue
		createUniqueConsumerTagName(), // consumer
		c.autoAck,                     // auto ack
		false,                         // exclusive
		false,                         // no local
		false,                         // no wait
		nil,                           // args
	)

	if err != nil {
		return err
	}

	logger.WithFields(log.Fields{
		"queue": c.queueName,
	}).Info("Consuming messages...")

	for m := range msgs {
		c.wg.Add(1)

		go func(msg amqplib.Delivery) {
			c.dispatch(msg)
			c.wg.Done()
		}(m)
	}

	return nil
}

// Listen start to listen for new messages.
func (c *consumer) Consume() {
	logger.Info("Registered handlers:")

	for _, handler := range c.handlers {
		logger.Infof("  %s", handler.action)
	}

	rs := c.conn.NotifyReestablish()

	for !c.closed {
		if !c.conn.IsConnected() {
			logger.Info("Connection not established. Waiting connection to be reestablished.")

			<-rs

			continue
		}

		err := c.doConsume()

		if err == nil {
			logger.WithFields(log.Fields{
				"queue":  c.queueName,
				"closed": c.closed,
			}).Info("Consumption finished.")
		} else {
			logger.WithFields(log.Fields{
				"queue": c.queueName,
				"error": err,
			}).Error("Error consuming events.")
		}
	}
}

func getAction(msg amqplib.Delivery) string {
	if ac, ok := msg.Headers["x-action-key"]; ok {
		return ac.(string)
	} else {
		return msg.RoutingKey
	}
}

func getXRetryCountHeader(msg amqplib.Delivery) (int32, bool) {
	if c, ok := msg.Headers["x-retry-count"]; ok {
		return c.(int32), true
	}

	return 0, false
}

func getXRetryDelayHeader(msg amqplib.Delivery) (time.Duration, bool) {
	if d, ok := msg.Headers["x-retry-delay"]; ok {
		t, err := time.ParseDuration(d.(string))
		if err == nil {
			return t, true
		}
	}

	return time.Duration(0), false
}
