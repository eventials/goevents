package amqp

import (
	"fmt"
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

	c.Subscribe("my_action_1", func(body []byte) error {
		func1 <- true
		return nil
	})

	c.Subscribe("my_action_2", func(body []byte) error {
		func2 <- true
		return nil
	})

	go c.Consume()

	p, err := NewProducer(conn, "events-exchange")

	assert.Nil(t, err)

	p.Publish("my_action_1", []byte(""))

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

	c, err := NewConsumer(conn, false, "webhooks", "event_publish_consumer")

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	c.Subscribe("webinar.*", func(body []byte) error {
		func1 <- true
		return nil
	})

	c.Subscribe("foobar.*", func(body []byte) error {
		func2 <- true
		return nil
	})

	go c.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("webinar.state_changed", []byte(""))

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

	c, err := NewConsumer(conn, false, "webhooks", "event_publish_consumer")

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	c.Subscribe("webinar.*", func(body []byte) error {
		func1 <- true
		return nil
	})

	c.Subscribe("webinar.state_changed", func(body []byte) error {
		func2 <- true
		return nil
	})

	go c.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("webinar.state_changed", []byte(""))

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

	c, err := NewConsumer(conn, false, "webhooks", "event_publish_consumer")

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	c.Subscribe("webinar.state_changed", func(body []byte) error {
		func1 <- true
		return nil
	})

	c.Subscribe("webinar.*", func(body []byte) error {
		func2 <- true
		return nil
	})

	go c.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("webinar.state_changed", []byte(""))

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

	c, err := NewConsumer(conn, false, "webhooks", "event_publish_consumer")

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	c.SubscribeWithOptions("my_action", func(body []byte) error {
		if calledOnce {
			called <- true
			return nil
		} else {
			calledOnce = true
			return fmt.Errorf("Error.")
		}
	}, 1*time.Second, false, 5)

	go c.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("my_action", []byte(""))

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

	c, err := NewConsumer(conn, false, "webhooks", "event_publish_consumer")

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	c.SubscribeWithOptions("my_action", func(body []byte) error {
		if calledOnce {
			called <- true
			return nil
		} else {
			calledOnce = true
			panic("this is a panic!")
		}
	}, 1*time.Second, false, 5)

	go c.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("my_action", []byte(""))

	select {
	case <-called:
	case <-time.After(5 * time.Second):
		assert.Fail(t, "timed out")
	}
}

func TestRequeueToTheSameQueue(t *testing.T) {
	calledTimes1 := 0
	calledTimes2 := 0

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c1, err := NewConsumer(conn, false, "webhooks", "event_publish_consumer_1")
	assert.Nil(t, err)

	c2, err := NewConsumer(conn, false, "webhooks", "event_publish_consumer_2")
	assert.Nil(t, err)

	defer c1.Close()
	defer c2.Close()

	// Clean all messages if any...
	consumer1 := c1.(*Consumer)
	consumer1.channel.QueuePurge(consumer1.queueName, false)

	consumer2 := c2.(*Consumer)
	consumer2.channel.QueuePurge(consumer2.queueName, false)

	c1.Subscribe("my_action", func(body []byte) error {
		calledTimes2++
		return nil
	})

	c2.SubscribeWithOptions("my_action", func(body []byte) error {
		defer func() { calledTimes1++ }()

		if calledTimes1 == 0 {
			return fmt.Errorf("Error.")
		} else {
			return nil
		}
	}, 1*time.Second, false, 5)

	go c1.Consume()
	go c2.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("my_action", []byte(""))

	select {
	case <-time.After(5 * time.Second):
		if calledTimes1 != 2 {
			assert.Fail(t, "Consumer 1 got wrong qtd of messages.")
		}

		if calledTimes2 != 1 {
			assert.Fail(t, "Consumer 2 got wrong qtd of messages.")
		}
	}
}
