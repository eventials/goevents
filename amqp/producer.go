package amqp

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/eventials/goevents/messaging"

	log "github.com/sirupsen/logrus"
	amqplib "github.com/streadway/amqp"
)

// ErrNotAcked indicated that published messages was not acked by RabbitMQ
var ErrNotAcked = errors.New("messge was not acked")

type message struct {
	action string
	msg    amqplib.Publishing
}

// producer holds a amqp connection and channel to publish messages to.
type producer struct {
	m             sync.Mutex
	conn          *connection
	channel       *amqp.Channel
	notifyConfirm chan amqp.Confirmation

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
		notifyConfirm: make(chan amqp.Confirmation),
	}

	err := producer.setupTopology()

	if err != nil {
		return nil, err
	}

	go producer.handleReestablishedConnnection()
	go producer.drainInternalQueue()

	return producer, err
}

// Publish publishes an action.
func (p *producer) Publish(action string, data []byte) {
	messageID, _ := NewUUIDv4()

	p.publishAmqMessage(action, amqplib.Publishing{
		MessageId:    messageID,
		DeliveryMode: amqplib.Persistent,
		Timestamp:    time.Now().UTC(),
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
	receiver := make(chan bool, 1)

	p.m.Lock()
	p.closes = append(p.closes, receiver)
	p.m.Unlock()

	return receiver
}

// Close the producer's internal queue.
func (p *producer) Close() {
	p.m.Lock()
	defer p.m.Unlock()

	p.closed = true
	close(p.internalQueue)

	p.channel.Close()
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (p *producer) changeChannel(channel *amqp.Channel) {
	p.channel = channel
	p.notifyConfirm = make(chan amqp.Confirmation)
	p.channel.NotifyPublish(p.notifyConfirm)
}

func (p *producer) setupTopology() error {
	log.WithFields(log.Fields{
		"type":     "goevents",
		"sub_type": "producer",
	}).Debug("Setting up topology...")

	p.m.Lock()
	defer p.m.Unlock()

	channel, err := p.conn.openChannel()

	if err != nil {
		return err
	}

	if p.exchangeName != "" {
		if err != nil {
			return err
		}

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

	err = channel.Confirm(false)

	if err != nil {
		err = fmt.Errorf("Channel could not be put into confirm mode: %s", err)
		return err
	}

	p.changeChannel(channel)

	log.WithFields(log.Fields{
		"type":     "goevents",
		"sub_type": "producer",
	}).Debug("Topology ready.")

	return nil
}

func (p *producer) handleReestablishedConnnection() {
	rs := p.conn.NotifyReestablish()

	for !p.closed {
		<-rs

		err := p.setupTopology()

		if err != nil {
			log.WithFields(log.Fields{
				"type":     "goevents",
				"sub_type": "producer",
				"error":    err,
			}).Error("Error setting up topology after reconnection.")
		}
	}
}

func (p *producer) publishMessage(msg amqplib.Publishing, queue string) (err error) {
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

	if !p.conn.IsConnected() {
		err = errors.New("connection is not open")
		return
	}

	err = p.channel.Publish(p.exchangeName, queue, false, false, msg)

	if err != nil {
		return
	}

	select {
	case confirm := <-p.notifyConfirm:
		if confirm.Ack {
			return
		}
	case <-time.After(p.config.publishInterval):
		err = ErrNotAcked
		return
	}

	return
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
			}).Debug("Publishing message to the exchange.")

			// block until confirmation
			err := p.publishMessage(m.msg, m.action)
			retry = err != nil

			if err != nil {
				log.WithFields(log.Fields{
					"action":     m.action,
					"body":       m.msg.Body,
					"message_id": m.msg.MessageId,
					"error":      err,
					"type":       "goevents",
					"sub_type":   "producer",
				}).Error("Error publishing message to the exchange. Retrying...")
			}
		}
	}

	for _, c := range p.closes {
		c <- true
	}
}
