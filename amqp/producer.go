package amqp

import (
	"sync"
	"time"

	"github.com/eventials/goevents/messaging"

	log "github.com/Sirupsen/logrus"
	amqplib "github.com/streadway/amqp"
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

	channel *amqplib.Channel

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

	p.channel, err = p.conn.OpenChannel()
	p.ackChannel, p.nackChannel = p.channel.NotifyConfirm(make(chan uint64, 1), make(chan uint64, 1))

	// put the channel in confirm mode.
	p.channel.Confirm(false)

	if err != nil {
		return err
	}

	err = p.channel.ExchangeDeclare(
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

func (p *Producer) drainInternalQueue() {
	for m := range p.internalQueue {
		for i := 0; !p.closed; i++ {
			messageId, _ := NewUUIDv4()

			msg := amqplib.Publishing{
				MessageId:    messageId,
				DeliveryMode: amqplib.Persistent,
				Timestamp:    time.Now(),
				Body:         m.data,
			}

			err := func() error {
				p.m.Lock()
				defer p.m.Unlock()

				log.WithFields(log.Fields{
					"type":     "goevents",
					"sub_type": "producer",
					"attempt":  i,
				}).Debug("Publishing message to the exchange.")

				return p.channel.Publish(p.exchangeName, m.action, false, false, msg)
			}()

			if err != nil {
				log.WithFields(log.Fields{
					"type":     "goevents",
					"sub_type": "producer",
					"error":    err,
					"attempt":  i,
				}).Error("Error publishing message to the exchange. Retrying...")

				time.Sleep(p.config.publishInterval)
				continue
			}

			select {
			case <-p.ackChannel:
				goto outer // 😈
			case <-p.nackChannel:
				log.WithFields(log.Fields{
					"type":     "goevents",
					"sub_type": "producer",
					"attempt":  i,
				}).Error("Error publishing message to the exchange. Retrying...")

				time.Sleep(p.config.publishInterval)
			}
		}
	outer:
	}

	for _, c := range p.closes {
		c <- true
	}
}
