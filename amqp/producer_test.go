package amqp

import (
	"testing"
	"time"

	"github.com/eventials/goevents/messaging"
	"github.com/stretchr/testify/assert"
)

func TestPublish(t *testing.T) {
	timesCalled := 0

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "webhooks", "TestPublish")

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	c.channel.QueuePurge(c.queueName, false)

	c.Subscribe("action.name", func(e messaging.Event) error {
		defer func() { timesCalled++ }()
		return nil
	}, nil)

	go c.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("action.name", []byte(""))

	select {
	case <-time.After(1 * time.Second):
		assert.Equal(t, 1, timesCalled, "Message wasn't published.")
	}
}

func TestPublishMultipleTimes(t *testing.T) {
	timesCalled := 0

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "webhooks", "TestPublishMultipleTimes")

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	c.channel.QueuePurge(c.queueName, false)

	c.Subscribe("action.name", func(e messaging.Event) error {
		defer func() { timesCalled++ }()
		return nil
	}, nil)

	go c.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	for i := 0; i < 5; i++ {
		p.Publish("action.name", []byte(""))
	}

	select {
	case <-time.After(1 * time.Second):
		assert.Equal(t, 5, timesCalled, "One or more messages weren't published.")
	}
}
