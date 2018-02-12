package amqp

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/eventials/goevents/messaging"

	log "github.com/sirupsen/logrus"
	amqplib "github.com/streadway/amqp"
)

var (
	ErrNotAcked = errors.New("Messge was not acked")
)

type message struct {
	action string
	msg    amqplib.Publishing
}

// producer holds a amqp connection and channel to publish messages to.
type producer struct {
	m    sync.Mutex
	conn *connection

	config ProducerConfig

	internalQueue chan message

	ackChannel  chan uint64
	nackChannel chan uint64

	exchangeName string

	closed bool
	closes []chan bool
}

// ProducerConfig to be used when creating a new producer.
type ProducerConfig struct {
	publishInterval time.Duration
}

// NewProducer returns a new AMQP Producer.
// Uses a default ProducerConfig with 2 second of publish interval.
func NewProducer(c messaging.Connection, exchange string) (*producer, error) {
	return NewProducerConfig(c, exchange, ProducerConfig{
		publishInterval: 2 * time.Second,
	})
}

// NewProducerConfig returns a new AMQP Producer.
func NewProducerConfig(c messaging.Connection, exchange string, config ProducerConfig) (*producer, error) {
	producer := &producer{
		conn:          c.(*connection),
		config:        config,
		internalQueue: make(chan message),
		exchangeName:  exchange,
	}

	err := producer.setupTopology()

	if err != nil {
		return nil, err
	} else {
		go producer.handleReestablishedConnnection()
		go producer.drainInternalQueue()

		return producer, err
	}
}

// Publish publishes an action.
func (p *producer) Publish(action string, data []byte) {
	messageId, _ := NewUUIDv4()

	p.publishAmqMessage(action, amqplib.Publishing{
		MessageId:    messageId,
		DeliveryMode: amqplib.Persistent,
		Timestamp:    time.Now(),
		Body:         data,
	})
}

func (p *producer) publishAmqMessage(queue string, msg amqplib.Publishing) {
	p.internalQueue <- message{
		action: queue,
		msg:    msg,
	}
}

// NotifyClose returns a channel to be notified then this producer closes.
func (p *producer) NotifyClose() <-chan bool {
	receiver := make(chan bool)
	p.closes = append(p.closes, receiver)

	return receiver
}

// Close the producer's internal queue.
func (p *producer) Close() {
	p.m.Lock()
	defer p.m.Unlock()

	p.closed = true
	close(p.internalQueue)
}

func (p *producer) setupTopology() error {
	log.WithFields(log.Fields{
		"type":     "goevents",
		"sub_type": "producer",
	}).Debug("Setting up topology...")

	p.m.Lock()
	defer p.m.Unlock()

	if p.exchangeName != "" {
		channel, err := p.conn.openChannel()

		if err != nil {
			return err
		}

		defer channel.Close()

		err = channel.ExchangeDeclare(
			p.exchangeName, // name
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
	}

	log.WithFields(log.Fields{
		"type":     "goevents",
		"sub_type": "producer",
	}).Debug("Topology ready.")

	return nil
}

func (p *producer) handleReestablishedConnnection() {
	for !p.closed {
		p.conn.WaitUntilConnectionReestablished()

		err := p.setupTopology()

		if err != nil {
			log.WithFields(log.Fields{
				"type":  "amqp",
				"error": err,
			}).Error("Error setting up topology after reconnection.")
		}
	}
}

func (p *producer) publishMessage(msg amqplib.Publishing, queue string) error {
	channel, err := p.conn.openChannel()

	if err != nil {
		return err
	}

	defer channel.Close()

	if err := channel.Confirm(false); err != nil {
		return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}

	confirms := channel.NotifyPublish(make(chan amqplib.Confirmation, 1))

	err = channel.Publish(p.exchangeName, queue, false, false, msg)

	if err != nil {
		return err
	} else {
		if confirmed := <-confirms; !confirmed.Ack {
			return ErrNotAcked
		}
	}

	return nil
}

func (p *producer) isClosed() bool {
	p.m.Lock()
	defer p.m.Unlock()

	return p.closed
}

func (p *producer) drainInternalQueue() {
	for m := range p.internalQueue {
		var retry = true

		for retry && !p.isClosed() {
			log.WithFields(log.Fields{
				"action":     m.action,
				"body":       m.msg.Body,
				"message_id": m.msg.MessageId,
				"type":       "goevents",
				"sub_type":   "producer",
				"exchange":   p.exchangeName,
			}).Info("Publishing message to the exchange.")

			err := p.publishMessage(m.msg, m.action)

			if err != nil {
				log.WithFields(log.Fields{
					"action":     m.action,
					"body":       m.msg.Body,
					"message_id": m.msg.MessageId,
					"error":      err,
					"type":       "goevents",
					"sub_type":   "producer",
				}).Error("Error publishing message to the exchange. Retrying...")

				time.Sleep(p.config.publishInterval)
				continue
			} else {
				retry = false
			}
		}
	}

	for _, c := range p.closes {
		c <- true
	}
}
