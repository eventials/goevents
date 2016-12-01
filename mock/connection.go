package mock

import (
	"github.com/eventials/goevents/messaging"
	"github.com/stretchr/testify/mock"
)

type Connection struct {
	mock.Mock
}

func (c *Connection) Consumer(autoAck bool) (messaging.Consumer, error) {
	args := c.Called(autoAck)
	return args.Get(0).(messaging.Consumer), args.Error(1)
}

func (c *Connection) Producer() (messaging.Producer, error) {
	args := c.Called()
	return args.Get(0).(*Producer), args.Error(1)
}

func (c *Connection) Close() {
	c.Called()
}
