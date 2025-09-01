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

// Producer holds a amqp connection and channel to publish messages to.
type producer struct {
	m               sync.RWMutex
	wg              sync.WaitGroup
	conn            *connection
	channel         *amqplib.Channel
	notifyConfirm   chan amqplib.Confirmation
	notifyChanClose chan *amqplib.Error
	config          ProducerConfig

	internalQueue chan message

	exchangeName string

	closed       bool
	channelReady bool
	closes       []chan bool

	// synchronization for (re)configuration and readiness gate
	readyCh  chan struct{}
	reconfMu sync.RWMutex
}

// ProducerConfig to be used when creating a new producer.
type ProducerConfig struct {
	PublishInterval time.Duration
	ConfirmTimeout  time.Duration
}

// NewProducer returns a new AMQP Producer.
// Uses a default ProducerConfig with 2 second of publish interval.
func NewProducer(c messaging.Connection, exchange string) (*producer, error) {
	return NewProducerConfig(c, exchange, ProducerConfig{
		PublishInterval: 2 * time.Second,
		ConfirmTimeout:  10 * time.Second,
	})
}

// NewProducerConfig returns a new AMQP Producer.
func NewProducerConfig(c messaging.Connection, exchange string, config ProducerConfig) (*producer, error) {
	producer := &producer{
		conn:          c.(*connection),
		config:        config,
		internalQueue: make(chan message),
		exchangeName:  exchange,
		readyCh:       make(chan struct{}), // initial gate blocked until topology is ready
	}

	// configure initial topology under lock and only then release the gate
	if err := producer.setupTopology(); err != nil {
		return nil, err
	}
	close(producer.readyCh)

	go producer.drainInternalQueue()
	go producer.handleReestablishedConnnection()

	return producer, nil
}

// Publish publishes an action.
func (p *producer) Publish(input messaging.MessageInput) {
	// ignore messages published to a closed producer
	if p.isClosed() {
		return
	}

	messageID, _ := NewUUIDv4()

	now := time.Now().UTC()

	p.publishAmqMessage(input.Action, amqplib.Publishing{
		MessageId:    messageID,
		DeliveryMode: amqplib.Persistent,
		Timestamp:    now,
		Body:         input.Data,
		Headers: amqplib.Table{
			"x-epoch-milli": int64(now.UnixNano()/int64(time.Nanosecond)) / int64(time.Millisecond),
		},
	})
}

func (p *producer) publishAmqMessage(queue string, msg amqplib.Publishing) {
	p.wg.Add(1)

	log.WithFields(log.Fields{
		"action":     queue,
		"message_id": msg.MessageId,
		"type":       "goevents",
		"sub_type":   "producer",
		"exchange":   p.exchangeName,
		"length":     len(p.internalQueue),
	}).Debug("Publishing message to internal queue.")

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

func (p *producer) notifyProducerClosed() {
	p.m.RLock()
	defer p.m.RUnlock()

	for _, c := range p.closes {
		c <- true
	}
}

// Close the producer's internal queue.
func (p *producer) Close() {
	p.setClosed()

	p.wg.Wait()

	close(p.internalQueue)

	if p.channel != nil {
		p.channel.Close()
	}

	p.notifyProducerClosed()
}

func (p *producer) setupTopology() error {
	log.WithFields(log.Fields{
		"type":     "goevents",
		"sub_type": "producer",
	}).Debug("Setting up topology...")

	// Prevents races with the drain and other reconnection paths
	p.reconfMu.Lock()
	defer p.reconfMu.Unlock()

	channel, err := p.conn.openChannel()
	if err != nil {
		return err
	}

	if p.exchangeName != "" {
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
			channel.Close()
			return err
		}
	}

	// Enable confirm mode BEFORE publishing anything
	err = channel.Confirm(false)
	if err != nil {
		channel.Close()
		err = fmt.Errorf("channel could not be put into confirm mode: %s", err)
		return err
	}

	// Atomic swap of the channel and listeners
	p.channel = channel
	p.notifyChanClose = make(chan *amqplib.Error, 1)
	p.channel.NotifyClose(p.notifyChanClose)
	p.notifyConfirm = make(chan amqplib.Confirmation, 1024)
	p.channel.NotifyPublish(p.notifyConfirm)
	p.setChannelReady(true)

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
	p.m.RLock()
	defer p.m.RUnlock()
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
		// true => connection dropped; false => only the channel dropped
		connectionLost := p.waitConnectionLost()

		if connectionLost {
			// Wait for physical reconnection of the connection
			<-rs
		}

		// Block publishing until topology is ready
		p.reconfMu.Lock()
		p.readyCh = make(chan struct{})
		p.reconfMu.Unlock()

		// Retry with backoff until topology is rebuilt
		for attempt := 0; ; attempt++ {
			err := p.setupTopology()
			if err == nil {
				close(p.readyCh)
				log.WithFields(log.Fields{
					"type":     "goevents",
					"sub_type": "producer",
				}).Info("Connection reestablished and topology configured.")
				break
			}

			log.WithFields(log.Fields{
				"type":     "goevents",
				"sub_type": "producer",
				"error":    err,
			}).Error("Error setting up topology after reconnection.")

			// use PublishInterval as base backoff
			sleep := p.config.PublishInterval
			if sleep <= 0 {
				sleep = 2 * time.Second
			}
			time.Sleep(sleep)
		}
	}
}

func (p *producer) publishMessage(msg amqplib.Publishing, queue string) (err error) {
	if !p.isConnected() {
		err = errors.New("connection/channel is not open")
		return
	}

	// Wait for readiness gate (protects against reconnection race)
	select {
	case <-p.readyCh:
		// ok
	default:
		// if not ready yet, wait blocking
		<-p.readyCh
	}

	logMessage := log.WithFields(log.Fields{
		"action":     queue,
		"message_id": msg.MessageId,
		"type":       "goevents",
		"sub_type":   "producer",
		"exchange":   p.exchangeName,
	})

	logMessage.WithFields(log.Fields{
		"body": msg.Body,
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
				err = errors.New("unknown panic")
			}
		}
	}()

	// atomic snapshot of the channel under read lock
	p.reconfMu.RLock()
	ch := p.channel
	nc := p.notifyConfirm
	p.reconfMu.RUnlock()

	err = ch.Publish(
		p.exchangeName, // Exchange
		queue,          // Routing key
		false,          // Mandatory
		false,          // Immediate
		msg)
	if err != nil {
		return
	}

	logMessage.Debug("Waiting message to be acked or timed out.")

	select {
	case confirm := <-nc:
		if confirm.Ack {
			return
		}
		err = ErrNotAcked
		return
	case <-time.After(p.config.ConfirmTimeout):
		err = ErrTimedout
		return
	}
}

func (p *producer) isClosed() bool {
	p.m.RLock()
	defer p.m.RUnlock()
	return p.closed
}

func (p *producer) drainInternalQueue() {
	for m := range p.internalQueue {
		retry := true

		for retry {
			// Wait for channel to be ready
			select {
			case <-p.readyCh:
				// proceed
			default:
				<-p.readyCh
			}

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

				if err == ErrTimedout {
					log.Warn("Closing producer channel due timeout wating msg confirmation")

					// force close to run setupTopology via handleReestablishedConnnection
					p.setChannelReady(false)
					if p.channel != nil {
						_ = p.channel.Close()
					}
				}

				time.Sleep(p.config.PublishInterval)
			} else {
				p.wg.Done()
				retry = false
			}
		}
	}
}
