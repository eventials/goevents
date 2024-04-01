package mock

import (
	"github.com/eventials/goevents/messaging"
	"github.com/stretchr/testify/mock"
)

type Consumer struct {
	mock.Mock
}

func NewMockConsumer() messaging.Consumer {
	return &Consumer{}
}

func (c *Consumer) Subscribe(action string, handler messaging.EventHandler, options *messaging.SubscribeOptions) error {
	args := c.Called(action, handler, options)
	return args.Error(0)
}

func (c *Consumer) SubscribeWithoutSNS(handlerFn messaging.EventHandler, options *messaging.SubscribeOptions) error {
	args := c.Called(handlerFn, options)
	return args.Error(0)
}

func (c *Consumer) Unsubscribe(action string) error {
	args := c.Called(action)
	return args.Error(0)
}

func (c *Consumer) Consume() {
	c.Called()
}

func (c *Consumer) Close() {
	c.Called()
}

func (c *Consumer) BindActions(actions ...string) error {
	args := c.Called(actions)
	return args.Error(0)
}

func (c *Consumer) UnbindActions(actions ...string) error {
	args := c.Called(actions)
	return args.Error(0)
}
