package amqp

import (
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	amqplib "github.com/streadway/amqp"

	"github.com/eventials/goevents/messaging"
)

// Connection with an AMQP peer.
type connection struct {
	config     ConnectionConfig
	m          sync.Mutex
	url        string
	connection *amqplib.Connection

	closed    bool
	connected int32

	reestablishs []chan bool
}

// ConnectionConfig to be used when creating a new connection.
type ConnectionConfig struct {
	ReconnectInterval time.Duration
}

// NewConnection returns an AMQP Connection.
// Uses a default ConnectionConfig with 2 second of reconnect interval.
func NewConnection(url string) (*connection, error) {
	return NewConnectionConfig(url, ConnectionConfig{
		ReconnectInterval: 2 * time.Second,
	})
}

// NewConnectionConfig returns an AMQP Connection.
func NewConnectionConfig(url string, config ConnectionConfig) (*connection, error) {
	connection := &connection{
		url:    url,
		config: config,
	}

	err := connection.dial()

	if err != nil {
		return nil, err
	}

	atomic.StoreInt32(&connection.connected, 1)

	go connection.handleConnectionClose()

	return connection, nil
}

// NotifyConnectionClose returns a channel to notify when the connection closes.
func (c *connection) NotifyConnectionClose() <-chan error {
	ch := make(chan error)

	go func() {
		ch <- <-c.connection.NotifyClose(make(chan *amqplib.Error))
	}()

	return ch
}

// NotifyReestablish returns a channel to notify when the connection is restablished.
func (c *connection) NotifyReestablish() <-chan bool {
	receiver := make(chan bool, 1)

	c.m.Lock()
	c.reestablishs = append(c.reestablishs, receiver)
	c.m.Unlock()

	return receiver
}

// Consumer returns an AMQP Consumer.
func (c *connection) Consumer(autoAck bool, exchange, queue string) (messaging.Consumer, error) {
	return NewConsumer(c, autoAck, exchange, queue)
}

// OpenChannel returns an AMQP Channel.
func (c *connection) openChannel() (*amqplib.Channel, error) {
	c.m.Lock()
	defer c.m.Unlock()

	return c.connection.Channel()
}

// Producer returns an AMQP Producer.
func (c *connection) Producer(exchange string) (messaging.Producer, error) {
	return NewProducer(c, exchange)
}

// Close closes the AMQP connection.
func (c *connection) Close() {
	c.m.Lock()
	defer c.m.Unlock()

	if !c.closed {
		c.closed = true
		c.connection.Close()
	}
}

func (c *connection) IsConnected() bool {
	return atomic.LoadInt32(&c.connected) > 0
}

func (c *connection) dial() error {
	conn, err := amqplib.Dial(c.url)

	c.m.Lock()
	c.connection = conn
	c.m.Unlock()

	return err
}

func (c *connection) handleConnectionClose() {
	for !c.closed {
		<-c.NotifyConnectionClose()

		atomic.StoreInt32(&c.connected, 0)

		for i := 0; !c.closed; i++ {
			err := c.dial()

			if err == nil {
				atomic.StoreInt32(&c.connected, 1)

				log.WithFields(log.Fields{
					"type":     "goevents",
					"sub_type": "connection",
					"attempt":  i,
				}).Info("Connection reestablished.")

				for _, c := range c.reestablishs {
					c <- true
				}

				break
			} else {
				log.WithFields(log.Fields{
					"type":     "goevents",
					"sub_type": "connection",
					"error":    err,
					"attempt":  i,
				}).Error("Error reestablishing connection. Retrying...")

				time.Sleep(c.config.ReconnectInterval)
			}
		}
	}
}
