package amqp

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/eventials/goevents/messaging"
	"github.com/stretchr/testify/assert"
)

const (
	SleepSetupTopology = 300 * time.Millisecond
)

var conn *connection

func TestMain(m *testing.M) {
	var err error
	conn, err = NewConnection("amqp://guest:guest@broker:5672/")

	if err != nil {
		panic(err)
	}

	defer conn.Close()

	os.Exit(m.Run())
}

func clearQueue(conn *connection, queueName string) error {
	channel, err := conn.openChannel()

	if err != nil {
		return err
	}

	defer channel.Close()

	// Clean all messages if any...
	_, err = channel.QueuePurge(queueName, false)

	return err
}

func TestSubscribeActions(t *testing.T) {
	func1 := make(chan bool)
	func2 := make(chan bool)

	c, err := NewConsumer(conn, false, "webhooks", "TestSubscribeActions")

	if assert.Nil(t, err) {
		defer c.Close()

		clearQueue(conn, c.queueName)

		c.Subscribe("my_action_1", func(e messaging.Event) error {
			func1 <- true
			return nil
		}, nil)

		c.Subscribe("my_action_2", func(e messaging.Event) error {
			func2 <- true
			return nil
		}, nil)

		go c.Consume()

		// take a time to setup topology
		time.Sleep(SleepSetupTopology)

		p, err := NewProducer(conn, "webhooks")

		assert.Nil(t, err)

		p.Publish("my_action_1", []byte(""))

		select {
		case <-func1:
		case <-func2:
			assert.Fail(t, "called wrong action")
		case <-time.After(3 * time.Second):
			assert.Fail(t, "timed out")
		}
	}
}

func TestSubscribeActionsByBindAfterConsume(t *testing.T) {
	func1 := make(chan bool)
	func2 := make(chan bool)

	c, err := NewConsumer(conn, false, "webhooks", "TestSubscribeActionsByBindAfterConsume")

	if assert.Nil(t, err) {
		defer c.Close()

		clearQueue(conn, c.queueName)

		go c.Consume()

		// take a time to setup topology
		time.Sleep(SleepSetupTopology)

		c.Subscribe("my_action_1", func(e messaging.Event) error {
			func1 <- true
			return nil
		}, nil)

		c.Subscribe("my_action_2", func(e messaging.Event) error {
			func2 <- true
			return nil
		}, nil)

		assert.NoError(t, c.BindActions("my_action_1", "my_action_2"))

		p, err := NewProducer(conn, "webhooks")

		assert.Nil(t, err)

		p.Publish("my_action_1", []byte(""))

		select {
		case <-func1:
		case <-func2:
			assert.Fail(t, "called wrong action")
		case <-time.After(3 * time.Second):
			assert.Fail(t, "timed out")
		}
	}
}

func TestSubscribeActionsUnbindAfterConsume(t *testing.T) {
	func1 := make(chan bool)
	func2 := make(chan bool)

	c, err := NewConsumer(conn, false, "webhooks", "TestSubscribeActionsUnbindAfterConsume")

	if assert.Nil(t, err) {
		defer c.Close()

		clearQueue(conn, c.queueName)

		c.Subscribe("my_action_1", func(e messaging.Event) error {
			func1 <- true
			return nil
		}, nil)

		c.Subscribe("my_action_2", func(e messaging.Event) error {
			func2 <- true
			return nil
		}, nil)

		go c.Consume()

		// take a time to setup topology
		time.Sleep(SleepSetupTopology)

		assert.NoError(t, c.UnbindActions("my_action_2"))

		p, err := NewProducer(conn, "webhooks")

		assert.Nil(t, err)

		p.Publish("my_action_2", []byte(""))

		select {
		case <-func1:
			assert.Fail(t, "called wrong action")
		case <-func2:
			assert.Fail(t, "called wrong action")
		case <-time.After(3 * time.Second):
		}
	}
}

func TestSubscribeWildcardActions(t *testing.T) {
	func1 := make(chan bool)
	func2 := make(chan bool)

	c, err := NewConsumer(conn, false, "webhooks", "TestSubscribeWildcardActions")

	if assert.Nil(t, err) {
		defer c.Close()

		clearQueue(conn, c.queueName)

		c.Subscribe("webinar.*", func(e messaging.Event) error {
			func1 <- true
			return nil
		}, nil)

		c.Subscribe("foobar.*", func(e messaging.Event) error {
			func2 <- true
			return nil
		}, nil)

		go c.Consume()

		// take a time to setup topology
		time.Sleep(SleepSetupTopology)

		p, err := NewProducer(conn, "webhooks")

		assert.Nil(t, err)

		p.Publish("webinar.state_changed", []byte(""))

		select {
		case <-func1:
		case <-func2:
			assert.Fail(t, "called wrong action")
		case <-time.After(3 * time.Second):
			assert.Fail(t, "timed out")
		}
	}

}

func TestSubscribeWildcardActionOrder1(t *testing.T) {
	func1 := make(chan bool)
	func2 := make(chan bool)

	c, err := NewConsumer(conn, false, "webhooks", "TestSubscribeWildcardActionOrder1")

	if assert.Nil(t, err) {
		defer c.Close()

		clearQueue(conn, c.queueName)

		c.Subscribe("webinar.*", func(e messaging.Event) error {
			func1 <- true
			return nil
		}, nil)

		c.Subscribe("webinar.state_changed", func(e messaging.Event) error {
			func2 <- true
			return nil
		}, nil)

		go c.Consume()

		// take a time to setup topology
		time.Sleep(SleepSetupTopology)

		p, err := NewProducer(conn, "webhooks")

		if assert.Nil(t, err) {
			defer p.Close()

			p.Publish("webinar.state_changed", []byte(""))

			select {
			case <-func1:
			case <-func2:
				assert.Fail(t, "called wrong action")
			case <-time.After(3 * time.Second):
				assert.Fail(t, "timed out")
			}
		}
	}
}

func TestSubscribeWildcardActionOrder2(t *testing.T) {
	func1 := make(chan bool)
	func2 := make(chan bool)

	c, err := NewConsumer(conn, false, "webhooks", "TestSubscribeWildcardActionOrder2")

	if assert.Nil(t, err) {
		defer c.Close()

		clearQueue(conn, c.queueName)

		c.Subscribe("webinar.state_changed", func(e messaging.Event) error {
			func1 <- true
			return nil
		}, nil)

		c.Subscribe("webinar.*", func(e messaging.Event) error {
			func2 <- true
			return nil
		}, nil)

		go c.Consume()

		// take a time to setup topology
		time.Sleep(SleepSetupTopology)

		p, err := NewProducer(conn, "webhooks")

		if assert.Nil(t, err) {
			defer p.Close()

			p.Publish("webinar.state_changed", []byte(""))

			select {
			case <-func1:
			case <-func2:
				assert.Fail(t, "called wrong action")
			case <-time.After(3 * time.Second):
				assert.Fail(t, "timed out")
			}
		}
	}
}

func TestDontRetryMessageIfFailsToProcess(t *testing.T) {
	timesCalled := 0

	c, err := NewConsumer(conn, false, "webhooks", "TestDontRetryMessageIfFailsToProcess")

	if assert.Nil(t, err) {
		defer c.Close()

		clearQueue(conn, c.queueName)

		c.Subscribe("my_action", func(e messaging.Event) error {
			defer func() { timesCalled++ }()

			if timesCalled == 0 {
				return fmt.Errorf("Error")
			}

			return nil
		}, nil)

		go c.Consume()

		// take a time to setup topology
		time.Sleep(SleepSetupTopology)

		p, err := NewProducer(conn, "webhooks")

		if assert.Nil(t, err) {
			defer p.Close()

			p.Publish("my_action", []byte(""))

			select {
			case <-time.After(3 * time.Second):
				assert.Equal(t, 1, timesCalled, "Consumer got wrong quantity of messages.")
			}
		}
	}
}

func TestRetryMessageIfFailsToProcess(t *testing.T) {
	timesCalled := 0

	c, err := NewConsumer(conn, false, "webhooks", "TestRetryMessageIfFailsToProcess")

	if assert.Nil(t, err) {
		defer c.Close()

		clearQueue(conn, c.queueName)

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

		// take a time to setup topology
		time.Sleep(SleepSetupTopology)

		p, err := NewProducer(conn, "webhooks")

		if assert.Nil(t, err) {
			defer p.Close()

			p.Publish("my_action", []byte(""))

			select {
			case <-time.After(3 * time.Second):
				assert.True(t, timesCalled >= 1 || timesCalled <= 5, "Consumer got wrong quantity of messages.")
			}
		}
	}
}

func TestRetryMessageIfPanicsToProcess(t *testing.T) {
	timesCalled := 0

	c, err := NewConsumer(conn, false, "webhooks", "TestRetryMessageIfPanicsToProcess")

	if assert.Nil(t, err) {
		defer c.Close()

		clearQueue(conn, c.queueName)

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

		// take a time to setup topology
		time.Sleep(SleepSetupTopology)

		p, err := NewProducer(conn, "webhooks")

		if assert.Nil(t, err) {
			defer p.Close()

			p.Publish("my_action", []byte(""))

			select {
			case <-time.After(3 * time.Second):
				assert.Equal(t, 2, timesCalled, "Consumer got wrong quantity of messages.")
			}
		}
	}
}

func TestRetryMessageToTheSameQueue(t *testing.T) {
	timesCalled1 := 0
	timesCalled2 := 0

	c1, err := NewConsumer(conn, false, "webhooks", "TestRetryMessageToTheSameQueue_1")
	assert.Nil(t, err)

	c2, err := NewConsumer(conn, false, "webhooks", "TestRetryMessageToTheSameQueue_2")
	assert.Nil(t, err)

	defer c1.Close()
	defer c2.Close()

	clearQueue(conn, c1.queueName)
	clearQueue(conn, c2.queueName)

	c1.Subscribe("my_action", func(e messaging.Event) error {
		timesCalled2++
		return nil
	}, nil)

	c2.Subscribe("my_action", func(e messaging.Event) error {
		defer func() { timesCalled1++ }()

		if timesCalled1 == 0 {
			return fmt.Errorf("timescalled zero")
		}

		return nil
	}, &messaging.SubscribeOptions{
		RetryDelay:   100 * time.Millisecond,
		DelayedRetry: false,
		MaxRetries:   5,
	})

	go c1.Consume()

	// take a time to setup topology
	time.Sleep(SleepSetupTopology)

	go c2.Consume()

	// take a time to setup topology
	time.Sleep(SleepSetupTopology)

	p, err := NewProducer(conn, "webhooks")

	if assert.Nil(t, err) {
		defer p.Close()

		p.Publish("my_action", []byte(""))

		select {
		case <-time.After(3 * time.Second):
			assert.Equal(t, 2, timesCalled1, "Consumer 1 got wrong quantity of messages.")
			assert.Equal(t, 1, timesCalled2, "Consumer 2 got wrong quantity of messages.")
		}
	}
}

func TestActionExitsMaxRetries(t *testing.T) {
	timesCalled := 0

	c, err := NewConsumer(conn, false, "webhooks", "TestActionExitsMaxRetries")
	assert.Nil(t, err)

	defer c.Close()

	clearQueue(conn, c.queueName)

	// It runs once and get an error, it will try five times more until it stops.
	c.Subscribe("my_action", func(e messaging.Event) error {
		defer func() { timesCalled++ }()
		return fmt.Errorf("error")
	}, &messaging.SubscribeOptions{
		RetryDelay:   100 * time.Millisecond,
		DelayedRetry: false,
		MaxRetries:   5,
	})

	go c.Consume()

	// take a time to setup topology
	time.Sleep(SleepSetupTopology)

	p, err := NewProducer(conn, "webhooks")

	if assert.Nil(t, err) {
		defer p.Close()

		p.Publish("my_action", []byte(""))

		select {
		case <-time.After(3 * time.Second):
			assert.True(t, timesCalled >= 4 || timesCalled <= 6, "Consumer got wrong quantity of messages.")
		}
	}
}

func TestActionExitsMaxRetriesWhenDelayed(t *testing.T) {
	c, err := NewConsumer(conn, false, "webhooks", "TestActionExitsMaxRetriesWhenDelayed")
	if assert.Nil(t, err) {
		defer c.Close()

		clearQueue(conn, c.queueName)

		timesCalled := 0

		// It runs once and get an error, it will try three times more until it stops.
		c.Subscribe("my_action", func(e messaging.Event) error {
			defer func() { timesCalled++ }()
			return fmt.Errorf("error")
		}, &messaging.SubscribeOptions{
			RetryDelay:   100 * time.Millisecond,
			DelayedRetry: true,
			MaxRetries:   3,
		})

		go c.Consume()

		// take a time to setup topology
		time.Sleep(SleepSetupTopology)

		p, err := NewProducer(conn, "webhooks")

		if assert.Nil(t, err) {
			defer p.Close()

			p.Publish("my_action", []byte(""))

			select {
			case <-time.After(3 * time.Second):
				assert.True(t, timesCalled > 1 || timesCalled <= 4, "Consumer got wrong quantity of messages.")
			}
		}
	}
}

func TestActionExitsMaxRetriesWhenDelayedWindow(t *testing.T) {
	timesCalled := 0

	c, err := NewConsumer(conn, false, "webhooks", "TestActionExitsMaxRetriesWhenDelayed")
	if assert.Nil(t, err) {
		defer c.Close()

		clearQueue(conn, c.queueName)

		// It runs once and get an error, it will try three times more until it stops.
		c.Subscribe("my_action", func(e messaging.Event) error {
			defer func() { timesCalled++ }()
			return fmt.Errorf("error")
		}, &messaging.SubscribeOptions{
			RetryDelay:   100 * time.Millisecond,
			DelayedRetry: true,
			MaxRetries:   5,
		})

		go c.Consume()

		// take a time to setup topology
		time.Sleep(SleepSetupTopology)

		p, err := NewProducer(conn, "webhooks")

		if assert.Nil(t, err) {
			defer p.Close()

			p.Publish("my_action", []byte(""))

			select {
			case <-time.After(3 * time.Second):
				assert.True(t, timesCalled > 1 || timesCalled <= 6, "Consumer got wrong quantity of messages.")
			}
		}
	}
}

func TestActionRetryTimeout(t *testing.T) {
	myActionTimesCalled := 0
	myAction2TimesCalled := 0

	c, err := NewConsumerConfig(conn, false, "webhooks", "TestActionRetryTimeout", ConsumerConfig{
		ConsumeRetryInterval: 2 * time.Second,
		PrefetchCount:        1,
	})

	if assert.Nil(t, err) {
		defer c.Close()

		clearQueue(conn, c.queueName)

		c.Subscribe("test1", func(e messaging.Event) error {
			defer func() {
				myActionTimesCalled++
			}()
			return fmt.Errorf("error")
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

		// take a time to setup topology
		time.Sleep(SleepSetupTopology)

		p, err := NewProducer(conn, "webhooks")

		if assert.Nil(t, err) {
			defer p.Close()

			p.Publish("test1", []byte(""))

			time.Sleep(200 * time.Millisecond)
			p.Publish("test2", []byte(""))

			select {
			case <-time.After(1 * time.Second):
				assert.True(t, myActionTimesCalled > 1 || myActionTimesCalled <= 4, "Consumer got wrong quantity of messages.")
				assert.Equal(t, 1, myAction2TimesCalled, "Consumer got wrong quantity of messages.")
			}
		}
	}
}

func TestConsumePrefetch(t *testing.T) {
	timesCalled := 0
	wait := make(chan bool)

	c, err := NewConsumerConfig(conn, false, "webhooks", "TestConsumePrefetch", ConsumerConfig{
		PrefetchCount:        5,
		ConsumeRetryInterval: 15 * time.Second,
	})

	if assert.Nil(t, err) {
		defer c.Close()

		clearQueue(conn, c.queueName)

		c.Subscribe("my_action", func(e messaging.Event) error {
			timesCalled++
			<-wait
			return nil
		}, nil)

		go c.Consume()

		// take a time to setup topology
		time.Sleep(SleepSetupTopology)

		p, err := NewProducer(conn, "webhooks")

		if assert.Nil(t, err) {
			defer p.Close()

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
	}
}

func TestBlankQueueWithPrefix(t *testing.T) {
	c, err := NewConsumerConfig(conn, false, "webhooks", "", ConsumerConfig{
		ConsumeRetryInterval: 2 * time.Second,
		PrefetchCount:        0,
		PrefixName:           "teste@",
	})

	if assert.Nil(t, err) {
		defer c.Close()

		fmt.Println(c.queueName)

		wait := make(chan bool)
		myActionTimesCalled := 0

		c.Subscribe("TestBlankQueueWithPrefix", func(e messaging.Event) error {
			myActionTimesCalled++
			wait <- true
			return nil
		}, &messaging.SubscribeOptions{
			RetryDelay:   100 * time.Millisecond,
			DelayedRetry: true,
			MaxRetries:   3,
		})

		go c.Consume()

		// take a time to setup topology
		time.Sleep(SleepSetupTopology)

		p, err := NewProducer(conn, "webhooks")

		if assert.Nil(t, err) {
			defer p.Close()

			p.Publish("TestBlankQueueWithPrefix", []byte(""))

			<-wait
			assert.Equal(t, 1, myActionTimesCalled, "Consumer got wrong quantity of messages.")
		}
	}
}

func TestCallEventAckMethod(t *testing.T) {
	func1 := make(chan bool)
	func2 := make(chan bool)

	c, err := NewConsumer(conn, false, "multi", "TestCallEventAckMethod")

	if assert.Nil(t, err) {
		defer c.Close()

		clearQueue(conn, c.queueName)

		c.Subscribe("multi", func(e messaging.Event) error {
			e.Manual()

			e.Ack(false)
			func1 <- true
			return nil
		}, nil)

		c.Subscribe("multi_2", func(e messaging.Event) error {
			e.Manual()

			e.Ack(false)
			func2 <- true
			return nil
		}, nil)

		go c.Consume()

		// take a time to setup topology
		time.Sleep(SleepSetupTopology)

		p, err := NewProducer(conn, "multi")

		assert.Nil(t, err)

		p.Publish("multi", []byte(""))

		select {
		case <-func1:
		case <-func2:
			assert.Fail(t, "called wrong action")
		case <-time.After(3 * time.Second):
			assert.Fail(t, "timed out")
		}
	}
}

func TestCallEventNackMethod(t *testing.T) {
	c, err := NewConsumer(conn, false, "onlynack", "TestCallEventNackMethod")

	if assert.Nil(t, err) {
		count := 0
		defer c.Close()
		clearQueue(conn, c.queueName)
		c.Subscribe("multi", func(e messaging.Event) error {
			e.Manual()

			count++

			if count == 3 {
				e.Ack(false)
			} else {
				e.Nack(false, true)
			}

			return nil
		}, nil)

		go c.Consume()

		// take a time to setup topology
		time.Sleep(SleepSetupTopology)

		p, err := NewProducer(conn, "onlynack")

		assert.Nil(t, err)

		p.Publish("multi", []byte(""))

		// Wait for requeue
		time.Sleep(10 * time.Second)

		assert.Equal(t, 3, count)
	}
}
