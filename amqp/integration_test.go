package amqp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPublishConsume(t *testing.T) {
	func1 := make(chan bool)
	func2 := make(chan bool)

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "events-exchange", "events-queue")

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	c.Subscribe("my_action_1", func(body []byte) bool {
		func1 <- true
		return true
	})

	c.Subscribe("my_action_2", func(body []byte) bool {
		func2 <- true
		return true
	})

	go c.Consume()

	p, err := NewProducer(conn, "events-exchange", "events-queue")

	assert.Nil(t, err)

	err = p.Publish("my_action_1", []byte(""))

	assert.Nil(t, err)

	select {
	case <-func1:
	case <-func2:
		assert.Fail(t, "called wrong action")
	case <-time.After(5 * time.Second):
		assert.Fail(t, "timed out")
	}
}

func TestPublishConsumeWildcardAction(t *testing.T) {
	func1 := make(chan bool)
	func2 := make(chan bool)

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "webhooks", "event_PublishConsumeer")

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	c.Subscribe("webinar.*", func(body []byte) bool {
		func1 <- true
		return true
	})

	c.Subscribe("foobar.*", func(body []byte) bool {
		func2 <- true
		return true
	})

	go c.Consume()

	p, err := NewProducer(conn, "webhooks", "event_PublishConsumeer")

	assert.Nil(t, err)

	err = p.Publish("webinar.state_changed", []byte(""))

	assert.Nil(t, err)

	select {
	case <-func1:
	case <-func2:
		assert.Fail(t, "called wrong action")
	case <-time.After(5 * time.Second):
		assert.Fail(t, "timed out")
	}
}

func TestPublishConsumeWildcardActionOrderMatters1(t *testing.T) {
	func1 := make(chan bool)
	func2 := make(chan bool)

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "webhooks", "event_PublishConsumeer")

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	c.Subscribe("webinar.*", func(body []byte) bool {
		func1 <- true
		return true
	})

	c.Subscribe("webinar.state_changed", func(body []byte) bool {
		func2 <- true
		return true
	})

	go c.Consume()

	p, err := NewProducer(conn, "webhooks", "event_PublishConsumeer")

	assert.Nil(t, err)

	err = p.Publish("webinar.state_changed", []byte(""))

	assert.Nil(t, err)

	select {
	case <-func1:
	case <-func2:
		assert.Fail(t, "called wrong action")
	case <-time.After(5 * time.Second):
		assert.Fail(t, "timed out")
	}
}

func TestPublishConsumeWildcardActionOrderMatters2(t *testing.T) {
	func1 := make(chan bool)
	func2 := make(chan bool)

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "webhooks", "event_PublishConsumeer")

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	c.Subscribe("webinar.state_changed", func(body []byte) bool {
		func1 <- true
		return true
	})

	c.Subscribe("webinar.*", func(body []byte) bool {
		func2 <- true
		return true
	})

	go c.Consume()

	p, err := NewProducer(conn, "webhooks", "event_PublishConsumeer")

	assert.Nil(t, err)

	err = p.Publish("webinar.state_changed", []byte(""))

	assert.Nil(t, err)

	select {
	case <-func1:
	case <-func2:
		assert.Fail(t, "called wrong action")
	case <-time.After(5 * time.Second):
		assert.Fail(t, "timed out")
	}
}

func TestPublishConsumeRequeueIfFail(t *testing.T) {
	calledOnce := false
	called := make(chan bool)

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "webhooks", "event_PublishConsumeer")

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	c.Subscribe("my_action", func(body []byte) bool {
		if calledOnce {
			called <- true
			return true
		} else {
			calledOnce = true
			return false
		}
	})

	go c.Consume()

	p, err := NewProducer(conn, "webhooks", "event_PublishConsumeer")

	assert.Nil(t, err)

	err = p.Publish("my_action", []byte(""))

	assert.Nil(t, err)

	select {
	case <-called:
	case <-time.After(5 * time.Second):
		assert.Fail(t, "timed out")
	}
}

func TestPublishConsumeRequeueIfPanic(t *testing.T) {
	calledOnce := false
	called := make(chan bool)

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "webhooks", "event_PublishConsumeer")

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	c.Subscribe("my_action", func(body []byte) bool {
		if calledOnce {
			called <- true
			return true
		} else {
			calledOnce = true
			panic("this is a panic!")
		}
	})

	go c.Consume()

	p, err := NewProducer(conn, "webhooks", "event_PublishConsumeer")

	assert.Nil(t, err)

	err = p.Publish("my_action", []byte(""))

	assert.Nil(t, err)

	select {
	case <-called:
	case <-time.After(5 * time.Second):
		assert.Fail(t, "timed out")
	}
}
