package amqp

import (
	"fmt"
	"github.com/eventials/goevents/messaging"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
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

	c.Subscribe("my_action_1", func(e messaging.Event) error {
		func1 <- true
		return nil
	}, nil)

	c.Subscribe("my_action_2", func(e messaging.Event) error {
		func2 <- true
		return nil
	}, nil)

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

	c.Subscribe("webinar.*", func(e messaging.Event) error {
		func1 <- true
		return nil
	}, nil)

	c.Subscribe("foobar.*", func(e messaging.Event) error {
		func2 <- true
		return nil
	}, nil)

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

	c.Subscribe("webinar.*", func(e messaging.Event) error {
		func1 <- true
		return nil
	}, nil)

	c.Subscribe("webinar.state_changed", func(e messaging.Event) error {
		func2 <- true
		return nil
	}, nil)

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

	c.Subscribe("webinar.state_changed", func(e messaging.Event) error {
		func1 <- true
		return nil
	}, nil)

	c.Subscribe("webinar.*", func(e messaging.Event) error {
		func2 <- true
		return nil
	}, nil)

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

func TestDontRetryMessageIfFailsToProcess(t *testing.T) {
	timesCalled := 0

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "webhooks", "TestDontRetryMessageIfFailsToProcess")

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	c.Subscribe("my_action", func(e messaging.Event) error {
		defer func() { timesCalled++ }()

		if timesCalled == 0 {
			return fmt.Errorf("Error")
		}

		return nil
	}, nil)

	go c.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("my_action", []byte(""))

	select {
	case <-time.After(1 * time.Second):
		assert.Equal(t, 1, timesCalled, "Consumer got wrong quantity of messages.")
	}
}

func TestRetryMessageIfFailsToProcess(t *testing.T) {
	timesCalled := 0

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "webhooks", "TestRetryMessageIfFailsToProcess")

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	c.Subscribe("my_action", func(e messaging.Event) error {
		defer func() { timesCalled++ }()

		if timesCalled == 0 {
			return fmt.Errorf("Error")
		}

		return nil
	}, &messaging.SubscribeOptions{
		RetryDelay:   100 * time.Millisecond,
		DelayedRetry: false,
		MaxRetries:   5,
	})

	go c.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("my_action", []byte(""))

	select {
	case <-time.After(1 * time.Second):
		assert.Equal(t, 2, timesCalled, "Consumer got wrong quantity of messages.")
	}
}

func TestRetryMessageIfPanicsToProcess(t *testing.T) {
	timesCalled := 0

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumer(conn, false, "webhooks", "TestRetryMessageIfPanicsToProcess")

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	c.Subscribe("my_action", func(e messaging.Event) error {
		defer func() { timesCalled++ }()

		if timesCalled == 0 {
			panic("this is a panic!")
		}

		return nil
	}, &messaging.SubscribeOptions{
		RetryDelay:   100 * time.Millisecond,
		DelayedRetry: false,
		MaxRetries:   5,
	})

	go c.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("my_action", []byte(""))

	select {
	case <-time.After(1 * time.Second):
		assert.Equal(t, 2, timesCalled, "Consumer got wrong quantity of messages.")
	}
}

func TestRetryMessageToTheSameQueue(t *testing.T) {
	timesCalled1 := 0
	timesCalled2 := 0

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c1, err := NewConsumer(conn, false, "webhooks", "TestRetryMessageToTheSameQueue_1")
	assert.Nil(t, err)

	c2, err := NewConsumer(conn, false, "webhooks", "TestRetryMessageToTheSameQueue_2")
	assert.Nil(t, err)

	defer c1.Close()
	defer c2.Close()

	// Clean all messages if any...
	consumer1 := c1.(*Consumer)
	consumer1.channel.QueuePurge(consumer1.queueName, false)

	consumer2 := c2.(*Consumer)
	consumer2.channel.QueuePurge(consumer2.queueName, false)

	c1.Subscribe("my_action", func(e messaging.Event) error {
		timesCalled2++
		return nil
	}, nil)

	c2.Subscribe("my_action", func(e messaging.Event) error {
		defer func() { timesCalled1++ }()

		if timesCalled1 == 0 {
			return fmt.Errorf("Error.")
		} else {
			return nil
		}
	}, &messaging.SubscribeOptions{
		RetryDelay:   100 * time.Millisecond,
		DelayedRetry: false,
		MaxRetries:   5,
	})

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
	c.Subscribe("my_action", func(e messaging.Event) error {
		defer func() { timesCalled++ }()
		return fmt.Errorf("Error.")
	}, &messaging.SubscribeOptions{
		RetryDelay:   100 * time.Millisecond,
		DelayedRetry: false,
		MaxRetries:   5,
	})

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

	// It runs once and get an error, it will try three times more until it stops.
	c.Subscribe("my_action", func(e messaging.Event) error {
		defer func() { timesCalled++ }()
		return fmt.Errorf("Error.")
	}, &messaging.SubscribeOptions{
		RetryDelay:   100 * time.Millisecond,
		DelayedRetry: true,
		MaxRetries:   3,
	})

	go c.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("my_action", []byte(""))

	select {
	case <-time.After(1 * time.Second):
		assert.Equal(t, 4, timesCalled, "Consumer got wrong quantity of messages.")
	}
}

func TestActionExitsMaxRetriesWhenDelayedWindow(t *testing.T) {
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

	// It runs once and get an error, it will try three times more until it stops.
	c.Subscribe("my_action", func(e messaging.Event) error {
		defer func() { timesCalled++ }()
		return fmt.Errorf("Error.")
	}, &messaging.SubscribeOptions{
		RetryDelay:   100 * time.Millisecond,
		DelayedRetry: true,
		MaxRetries:   5,
	})

	go c.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("my_action", []byte(""))

	select {
	case <-time.After(1 * time.Second):
		assert.True(t, timesCalled == 4 || timesCalled == 5, "Consumer got wrong quantity of messages.")
	}
}

func TestActionRetryTimeout(t *testing.T) {
	myActionTimesCalled := 0
	myAction2TimesCalled := 0

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumerConfig(conn, false, "webhooks", "TestActionRetryTimeout", ConsumerConfig{
		ConsumeRetryInterval:      2 * time.Second,
		PrefetchCount:             1,
		RetryTimeoutBeforeRequeue: 650 * time.Millisecond,
	})

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	c.Subscribe("test1", func(e messaging.Event) error {
		defer func() {
			myActionTimesCalled++
		}()
		return fmt.Errorf("Error.")
	}, &messaging.SubscribeOptions{
		RetryDelay:   300 * time.Millisecond,
		DelayedRetry: true,
		MaxRetries:   4,
	})

	c.Subscribe("test2", func(e messaging.Event) error {
		defer func() {
			myAction2TimesCalled++
		}()
		return nil
	}, nil)

	go c.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	p.Publish("test1", []byte(""))

	time.Sleep(200 * time.Millisecond)
	p.Publish("test2", []byte(""))

	select {
	case <-time.After(1 * time.Second):
		assert.Equal(t, 3, myActionTimesCalled, "Consumer got wrong quantity of messages.")
		assert.Equal(t, 1, myAction2TimesCalled, "Consumer got wrong quantity of messages.")
	}
}

func TestConsumePrefetch(t *testing.T) {
	timesCalled := 0
	wait := make(chan bool)

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	assert.Nil(t, err)

	defer conn.Close()

	c, err := NewConsumerConfig(conn, false, "webhooks", "TestConsumePrefetch", ConsumerConfig{
		PrefetchCount:        5,
		ConsumeRetryInterval: 15 * time.Second,
	})

	assert.Nil(t, err)

	defer c.Close()

	// Clean all messages if any...
	consumer := c.(*Consumer)
	consumer.channel.QueuePurge(consumer.queueName, false)

	c.Subscribe("my_action", func(e messaging.Event) error {
		timesCalled++
		<-wait
		return nil
	}, nil)

	go c.Consume()

	p, err := NewProducer(conn, "webhooks")

	assert.Nil(t, err)

	for i := 0; i < 10; i++ {
		p.Publish("my_action", []byte(""))
	}

	<-time.After(100 * time.Millisecond)
	assert.Equal(t, 5, timesCalled, "Consumer got wrong quantity of messages.")

	// release one
	wait <- true

	<-time.After(100 * time.Millisecond)
	assert.Equal(t, 6, timesCalled, "Consumer got wrong quantity of messages.")

	// release all
	for i := 0; i < 5; i++ {
		wait <- true
	}

	<-time.After(100 * time.Millisecond)
	assert.Equal(t, 10, timesCalled, "Consumer got wrong quantity of messages.")

	// release all
	for i := 0; i < 4; i++ {
		wait <- true
	}
}
