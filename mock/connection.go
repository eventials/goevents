package mock

import (
	"github.com/eventials/goevents/messaging"
	"github.com/stretchr/testify/mock"
)

type Connection struct {
	mock.Mock
}

func NewMockConnection() messaging.Connection {
	return &Connection{}
}

func (c *Connection) Consumer(autoAck bool, exchange, queue string) (messaging.Consumer, error) {
	args := c.Called(autoAck, exchange, queue)
	return args.Get(0).(messaging.Consumer), args.Error(1)
}

func (c *Connection) Producer(exchange string) (messaging.Producer, error) {
	args := c.Called(exchange)
	return args.Get(0).(messaging.Producer), args.Error(1)
}

func (c *Connection) Close() {
	c.Called()
}

func (c *Connection) NotifyConnectionClose() <-chan error {
	args := c.Called()
	return args.Get(0).(chan error)
}

func (c *Connection) NotifyReestablish() <-chan bool {
	args := c.Called()
	return args.Get(0).(chan bool)
}

func (c *Connection) WaitUntilConnectionCloses() {
	c.Called()
}

func (c *Connection) IsConnected() bool {
	args := c.Called()
	return args.Get(0).(bool)
}
