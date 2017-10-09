package amqp

import (
	"errors"
	"fmt"
	"github.com/eventials/goevents/messaging"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	amqplib "github.com/streadway/amqp"
)

var (
	ErrNotAcked = errors.New("Messge was not acked")
)

type message struct {
	action string
	data   []byte
}

// Producer holds a amqp connection and channel to publish messages to.
type Producer struct {
	m    sync.Mutex
	conn *Connection

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
func NewProducer(c messaging.Connection, exchange string) (messaging.Producer, error) {
	return NewProducerConfig(c, exchange, ProducerConfig{
		publishInterval: 2 * time.Second,
	})
}

// NewProducerConfig returns a new AMQP Producer.
func NewProducerConfig(c messaging.Connection, exchange string, config ProducerConfig) (messaging.Producer, error) {
	producer := &Producer{
		conn:          c.(*Connection),
		config:        config,
		internalQueue: make(chan message),
		exchangeName:  exchange,
	}

	err := producer.setupTopology()

	go producer.handleReestablishedConnnection()
	go producer.drainInternalQueue()

	return producer, err
}

// Publish publishes an action.
func (p *Producer) Publish(action string, data []byte) {
	p.internalQueue <- message{action, data}
}

// NotifyClose returns a channel to be notified then this producer closes.
func (p *Producer) NotifyClose() <-chan bool {
	receiver := make(chan bool)
	p.closes = append(p.closes, receiver)

	return receiver
}

// Close the producer's internal queue.
func (p *Producer) Close() {
	p.m.Lock()
	defer p.m.Unlock()

	p.closed = true
	close(p.internalQueue)
}

func (p *Producer) setupTopology() error {
	log.WithFields(log.Fields{
		"type":     "goevents",
		"sub_type": "producer",
	}).Debug("Setting up topology...")

	p.m.Lock()
	defer p.m.Unlock()

	var err error

	channel, err := p.conn.OpenChannel()

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

	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"type":     "goevents",
		"sub_type": "producer",
	}).Debug("Topology ready.")

	return nil
}

func (p *Producer) handleReestablishedConnnection() {
	reestablishChannel := p.conn.NotifyReestablish()

	for !p.closed {
		<-reestablishChannel

		err := p.setupTopology()

		if err != nil {
			log.WithFields(log.Fields{
				"type":  "amqp",
				"error": err,
			}).Error("Error setting up topology after reconnection.")
		}
	}
}

func (p *Producer) publishMessage(msg amqplib.Publishing, action string) error {
	channel, err := p.conn.OpenChannel()

	if err != nil {
		return err
	}

	defer channel.Close()

	if err := channel.Confirm(false); err != nil {
		return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}

	confirms := channel.NotifyPublish(make(chan amqplib.Confirmation, 1))

	err = channel.Publish(p.exchangeName, action, false, false, msg)

	if err != nil {
		return err
	} else {
		if confirmed := <-confirms; !confirmed.Ack {
			return ErrNotAcked
		}
	}

	return nil
}

func (p *Producer) isClosed() bool {
	p.m.Lock()
	defer p.m.Unlock()

	return p.closed
}

func (p *Producer) drainInternalQueue() {
	for m := range p.internalQueue {
		var retry = true
		for retry && !p.isClosed() {
			messageId, _ := NewUUIDv4()

			msg := amqplib.Publishing{
				MessageId:    messageId,
				DeliveryMode: amqplib.Persistent,
				Timestamp:    time.Now(),
				Body:         m.data,
			}

			log.WithFields(log.Fields{
				"type":     "goevents",
				"sub_type": "producer",
			}).Info("Publishing message to the exchange.")

			err := p.publishMessage(msg, m.action)

			if err != nil {
				log.WithFields(log.Fields{
					"type":     "goevents",
					"sub_type": "producer",
					"error":    err,
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
