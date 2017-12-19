package amqp

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	amqplib "github.com/streadway/amqp"

	"github.com/eventials/goevents/messaging"
)

// Connection with an AMQP peer.
type Connection struct {
	config     ConnectionConfig
	m          sync.Mutex
	url        string
	connection *amqplib.Connection
	closed     bool
	connected  bool

	reestablishs []chan bool
}

// ConnectionConfig to be used when creating a new connection.
type ConnectionConfig struct {
	reconnectInterval time.Duration
}

// NewConnection returns an AMQP Connection.
// Uses a default ConnectionConfig with 2 second of reconnect interval.
func NewConnection(url string) (messaging.Connection, error) {
	return NewConnectionConfig(url, ConnectionConfig{
		reconnectInterval: 2 * time.Second,
	})
}

// NewConnectionConfig returns an AMQP Connection.
func NewConnectionConfig(url string, config ConnectionConfig) (messaging.Connection, error) {
	connection := &Connection{
		url:    url,
		config: config,
	}

	err := connection.dial()

	if err != nil {
		return nil, err
	}

	connection.setConnected(true)

	go connection.handleConnectionClose()

	return connection, nil
}

// NotifyConnectionClose returns a channel to notify when the connection closes.
func (c *Connection) NotifyConnectionClose() <-chan error {
	ch := make(chan error)

	go func() {
		ch <- <-c.connection.NotifyClose(make(chan *amqplib.Error))
	}()

	return ch
}

// NotifyReestablish returns a channel to notify when the connection is restablished.
func (c *Connection) NotifyReestablish() <-chan bool {
	receiver := make(chan bool)
	c.reestablishs = append(c.reestablishs, receiver)

	return receiver
}

// Consumer returns an AMQP Consumer.
func (c *Connection) Consumer(autoAck bool, exchange, queue string) (messaging.Consumer, error) {
	return NewConsumer(c, autoAck, exchange, queue)
}

// OpenChannel returns an AMQP Consumer.
func (c *Connection) OpenChannel() (*amqplib.Channel, error) {
	c.m.Lock()
	defer c.m.Unlock()

	return c.connection.Channel()
}

// Producer returns an AMQP Producer.
func (c *Connection) Producer(exchange string) (messaging.Producer, error) {
	return NewProducer(c, exchange)
}

// Close closes the AMQP connection.
func (c *Connection) Close() {
	c.m.Lock()
	defer c.m.Unlock()

	c.closed = true
	c.connection.Close()
}

func (c *Connection) IsConnected() bool {
	c.m.Lock()
	defer c.m.Unlock()

	return c.connected
}

// WaitUntilConnectionCloses holds the execution until the connection closes.
func (c *Connection) WaitUntilConnectionCloses() {
	<-c.NotifyConnectionClose()
}

func (c *Connection) dial() error {
	conn, err := amqplib.Dial(c.url)

	c.m.Lock()
	defer c.m.Unlock()

	c.connection = conn

	return err
}

func (c *Connection) setConnected(connected bool) {
	c.m.Lock()
	defer c.m.Unlock()

	c.connected = connected
}

func (c *Connection) handleConnectionClose() {
	for !c.closed {
		c.WaitUntilConnectionCloses()
		c.setConnected(false)

		for i := 0; !c.closed; i++ {
			err := c.dial()

			if err == nil {
				c.setConnected(true)

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

				time.Sleep(c.config.reconnectInterval)
			}
		}
	}
}
