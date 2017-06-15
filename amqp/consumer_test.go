package amqp

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSubscribeActions(t *testing.T) {
	func1 := make(chan bool)
	func2 := make(chan bool)

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "webhooks", "TestSubscribeActions")

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

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("my_action_1", []byte(""))

	select {
	case <-func1:
	case <-func2:
		assert.Fail(t, "called wrong action")
	case <-time.After(1 * time.Second):
		assert.Fail(t, "timed out")
	}
}

func TestSubscribeWildcardActions(t *testing.T) {
	func1 := make(chan bool)
	func2 := make(chan bool)

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "webhooks", "TestSubscribeWildcardActions")

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
	case <-time.After(1 * time.Second):
		assert.Fail(t, "timed out")
	}
}

func TestSubscribeWildcardActionOrder1(t *testing.T) {
	func1 := make(chan bool)
	func2 := make(chan bool)

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "webhooks", "TestSubscribeWildcardActionOrder1")

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
	case <-time.After(1 * time.Second):
		assert.Fail(t, "timed out")
	}
}

func TestSubscribeWildcardActionOrder2(t *testing.T) {
	func1 := make(chan bool)
	func2 := make(chan bool)

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "webhooks", "TestSubscribeWildcardActionOrder2")

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
	case <-time.After(1 * time.Second):
		assert.Fail(t, "timed out")
	}
}

func TestDontRequeueMessageIfFailsToProcess(t *testing.T) {
	timesCalled := 0

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "webhooks", "TestDontRequeueMessageIfFailsToProcess")

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	c.Subscribe("my_action", func(body []byte) error {
		defer func() { timesCalled++ }()

		if timesCalled == 0 {
			return fmt.Errorf("Error")
		}

		return nil
	})

	go c.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("my_action", []byte(""))

	select {
	case <-time.After(1 * time.Second):
		assert.Equal(t, 1, timesCalled, "Consumer got wrong quantity of messages.")
	}
}

func TestRequeueMessageIfFailsToProcess(t *testing.T) {
	timesCalled := 0

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "webhooks", "TestRequeueMessageIfFailsToProcess")

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	c.SubscribeWithOptions("my_action", func(body []byte) error {
		defer func() { timesCalled++ }()

		if timesCalled == 0 {
			return fmt.Errorf("Error")
		}

		return nil
	}, 100*time.Millisecond, false, 5)

	go c.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("my_action", []byte(""))

	select {
	case <-time.After(1 * time.Second):
		assert.Equal(t, 2, timesCalled, "Consumer got wrong quantity of messages.")
	}
}

func TestRequeueMessageIfPanicsToProcess(t *testing.T) {
	timesCalled := 0

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "webhooks", "TestRequeueMessageIfPanicsToProcess")

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	c.SubscribeWithOptions("my_action", func(body []byte) error {
		defer func() { timesCalled++ }()

		if timesCalled == 0 {
			panic("this is a panic!")
		}

		return nil
	}, 100*time.Millisecond, false, 5)

	go c.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("my_action", []byte(""))

	select {
	case <-time.After(1 * time.Second):
		assert.Equal(t, 2, timesCalled, "Consumer got wrong quantity of messages.")
	}
}

func TestRequeueMessageToTheSameQueue(t *testing.T) {
	timesCalled1 := 0
	timesCalled2 := 0

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c1, err := NewConsumer(conn, false, "webhooks", "TestRequeueMessageToTheSameQueue_1")
	assert.Nil(t, err)

	c2, err := NewConsumer(conn, false, "webhooks", "TestRequeueMessageToTheSameQueue_2")
	assert.Nil(t, err)

	defer c1.Close()
	defer c2.Close()

	// Clean all messages if any...
	consumer1 := c1.(*Consumer)
	consumer1.channel.QueuePurge(consumer1.queueName, false)

	consumer2 := c2.(*Consumer)
	consumer2.channel.QueuePurge(consumer2.queueName, false)

	c1.Subscribe("my_action", func(body []byte) error {
		timesCalled2++
		return nil
	})

	c2.SubscribeWithOptions("my_action", func(body []byte) error {
		defer func() { timesCalled1++ }()

		if timesCalled1 == 0 {
			return fmt.Errorf("Error.")
		} else {
			return nil
		}
	}, 100*time.Millisecond, false, 5)

	go c1.Consume()
	go c2.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("my_action", []byte(""))

	select {
	case <-time.After(1 * time.Second):
		assert.Equal(t, 2, timesCalled1, "Consumer 1 got wrong quantity of messages.")
		assert.Equal(t, 1, timesCalled2, "Consumer 2 got wrong quantity of messages.")
	}
}

func TestActionExitsMaxRetries(t *testing.T) {
	timesCalled := 0

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "webhooks", "TestActionExitsMaxRetries")
	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	// It runs once and get an error, it will try five times more until it stops.
	c.SubscribeWithOptions("my_action", func(body []byte) error {
		defer func() { timesCalled++ }()
		return fmt.Errorf("Error.")
	}, 100*time.Millisecond, false, 5)

	go c.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("my_action", []byte(""))

	select {
	case <-time.After(1 * time.Second):
		assert.Equal(t, 6, timesCalled, "Consumer got wrong quantity of messages.")
	}
}

func TestActionExitsMaxRetriesWhenDelayed(t *testing.T) {
	timesCalled := 0

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "webhooks", "TestActionExitsMaxRetriesWhenDelayed")
	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	// It runs once and get an error, it will try five times more until it stops.
	c.SubscribeWithOptions("my_action", func(body []byte) error {
		defer func() { timesCalled++ }()
		return fmt.Errorf("Error.")
	}, 100*time.Millisecond, true, 3)

	go c.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("my_action", []byte(""))

	select {
	case <-time.After(1 * time.Second):
		assert.Equal(t, 4, timesCalled, "Consumer got wrong quantity of messages.")
	}
}
