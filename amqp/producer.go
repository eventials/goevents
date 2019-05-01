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
var ErrNotAcked = errors.New("message was not acked")
var ErrTimedout = errors.New("message was timed out")

type message struct {
	action string
	msg    amqplib.Publishing
}

// producer holds a amqp connection and channel to publish messages to.
type producer struct {
	m               sync.Mutex
	wg              sync.WaitGroup
	conn            *connection
	channel         *amqplib.Channel
	notifyConfirm   chan amqplib.Confirmation
	notifyChanClose chan *amqplib.Error
	closeQueue      chan bool
	config          ProducerConfig

	internalQueue chan message

	exchangeName string

	closed       bool
	channelReady bool
	closes       []chan bool
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
		internalQueue: make(chan message, 2),
		exchangeName:  exchange,
		closeQueue:    make(chan bool),
	}

	err := producer.setupTopology()

	if err != nil {
		return nil, err
	}

	go producer.drainInternalQueue()
	go producer.handleReestablishedConnnection()

	return producer, err
}

// Publish publishes an action.
func (p *producer) Publish(action string, data []byte) {
	// ignore messages published to a closed producer
	if p.isClosed() {
		return
	}

	messageID, _ := NewUUIDv4()

	p.publishAmqMessage(action, amqplib.Publishing{
		MessageId:    messageID,
		DeliveryMode: amqplib.Persistent,
		Timestamp:    time.Now().UTC(),
		Body:         data,
	})
}

func (p *producer) publishAmqMessage(queue string, msg amqplib.Publishing) {
	p.wg.Add(1)

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

func (p *producer) setClosed() {
	p.m.Lock()
	defer p.m.Unlock()

	p.closed = true
}

// Close the producer's internal queue.
func (p *producer) Close() {
	p.setClosed()

	p.wg.Wait()

	p.closeQueue <- true

	close(p.internalQueue)
	close(p.closeQueue)

	p.channel.Close()

	for _, c := range p.closes {
		c <- true
	}
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (p *producer) changeChannel(channel *amqplib.Channel) {
	p.channel = channel

	p.notifyChanClose = make(chan *amqplib.Error)
	p.channel.NotifyClose(p.notifyChanClose)

	p.notifyConfirm = make(chan amqplib.Confirmation)
	p.channel.NotifyPublish(p.notifyConfirm)

	p.channelReady = true
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

func (p *producer) setChannelReady(ready bool) {
	p.m.Lock()
	defer p.m.Unlock()

	p.channelReady = ready
}

func (p *producer) isChannelReady() bool {
	p.m.Lock()
	defer p.m.Unlock()

	return p.channelReady
}

func (p *producer) isConnected() bool {
	if !p.conn.IsConnected() {
		return false
	}

	return p.isChannelReady()
}

func (p *producer) waitConnectionLost() bool {
	if !p.isConnected() {
		return true
	}

	defer p.setChannelReady(false)

	select {
	case <-p.conn.NotifyConnectionClose():
		log.Warn("Producer connection closed")
		return true
	case <-p.notifyChanClose:
		log.Warn("Producer channel closed")
		return false
	}
}

func (p *producer) handleReestablishedConnnection() {
	rs := p.conn.NotifyReestablish()

	for !p.isClosed() {
		// true if connection is lot
		// false if channel connection is lost
		connectionLost := p.waitConnectionLost()

		if connectionLost {
			// Wait reconnect
			<-rs
		}

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
	if !p.isConnected() {
		err = errors.New("connection/channel is not open")
		return
	}

	log.WithFields(log.Fields{
		"action":     queue,
		"body":       msg.Body,
		"message_id": msg.MessageId,
		"type":       "goevents",
		"sub_type":   "producer",
		"exchange":   p.exchangeName,
	}).Debug("Publishing message to the exchange.")

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

	err = p.channel.Publish(
		p.exchangeName, // Exchange
		queue,          // Routing key
		false,          // Mandatory
		false,          // Immediate
		msg)

	if err != nil {
		return
	}

	select {
	case confirm := <-p.notifyConfirm:
		if confirm.Ack {
			return
		}

		err = ErrNotAcked
		return
	case <-time.After(p.config.publishInterval):
		err = ErrTimedout
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
	for {
		select {
		case <-p.closeQueue:
			return
		case m := <-p.internalQueue:
			retry := true

			for retry {
				// block until confirmation
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
				} else {
					p.wg.Done()
					retry = false
				}
			}
		}
	}
}
