package mock

import (
	"github.com/eventials/goevents/messaging"
	"github.com/stretchr/testify/mock"
)

type Consumer struct {
	mock.Mock
}

func (c *Consumer) Subscribe(action string, handler messaging.EventHandler) error {
	args := c.Called(action, handler)
	return args.Error(1)
}

func (c *Consumer) Unsubscribe(action string) error {
	args := c.Called(action)
	return args.Error(1)
}

func (c *Consumer) Listen() error {
	args := c.Called()
	return args.Error(1)
}

func (c *Consumer) ListenForever() error {
	args := c.Called()
	return args.Error(1)
}
