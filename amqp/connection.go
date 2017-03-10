package amqp

import (
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
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

	reestablishs []chan bool
}

// ConnectionConfig to be used when creating a new connection.
type ConnectionConfig struct {
	reconnectInterval time.Duration
	reconnectRetries  int
}

// NewConnection returns an AMQP Connection.
// Uses a default ConnectionConfig with 2 second of reconnect interval and 10 retries.
func NewConnection(url string) (messaging.Connection, error) {
	return NewConnectionConfig(url, ConnectionConfig{
		reconnectInterval: 2 * time.Second,
		reconnectRetries:  10,
	})
}

// NewConnectionConfig returns an AMQP Connection.
func NewConnectionConfig(url string, config ConnectionConfig) (messaging.Connection, error) {
	conn, err := amqplib.Dial(url)

	if err != nil {
		return nil, err
	}

	connection := &Connection{
		url:        url,
		connection: conn,
		config:     config,
	}

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

// WaitUntilConnectionCloses holds the execution until the connection closes.
func (c *Connection) WaitUntilConnectionCloses() {
	<-c.NotifyConnectionClose()
}

func (c *Connection) reestablish() error {
	conn, err := amqplib.Dial(c.url)

	c.m.Lock()
	defer c.m.Unlock()

	c.connection = conn

	return err

}

func (c *Connection) handleConnectionClose() {
	for !c.closed {
		c.WaitUntilConnectionCloses()

		for i := 1; i <= c.config.reconnectRetries; i++ {
			err := c.reestablish()

			if err == nil {
				log.WithFields(log.Fields{
					"type":        "amqp",
					"attempt":     i,
					"max_retries": c.config.reconnectRetries,
				}).Info("Connection reestablished")

				for _, c := range c.reestablishs {
					c <- true
				}

				break
			} else {
				if i < c.config.reconnectRetries {
					log.WithFields(log.Fields{
						"type":        "amqp",
						"error":       err,
						"attempt":     i,
						"max_retries": c.config.reconnectRetries,
					}).Error("Error reestablishing connection. Retrying...")

					time.Sleep(c.config.reconnectInterval)
				} else {
					log.WithFields(log.Fields{
						"type":        "amqp",
						"error":       err,
						"attempt":     i,
						"max_retries": c.config.reconnectRetries,
					}).Panic("Error reestablishing connection. Max retries reached, giving up...")
				}
			}
		}
	}
}
