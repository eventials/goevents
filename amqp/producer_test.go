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

	if assert.Nil(t, err) {
		defer conn.Close()

		channel, err := conn.openChannel()

		if assert.Nil(t, err) {
			defer channel.Close()

			c, err := NewConsumer(conn, false, "webhooks", "TestPublish")

			if assert.Nil(t, err) {
				defer c.Close()

				// Clean all messages if any...
				channel.QueuePurge(c.queueName, false)

				c.Subscribe("action.name", func(e messaging.Event) error {
					defer func() { timesCalled++ }()
					return nil
				}, nil)

				go c.Consume()

				// take a time to setup topology
				time.Sleep(SleepSetupTopology)

				p, err := NewProducer(conn, "webhooks")

				if assert.Nil(t, err) {
					input := messaging.MessageInput{
						Action:         "action.name",
						Data:           []byte(""),
						MessageGroupID: nil,
					}
					p.Publish(input)

					time.Sleep(1 * time.Second)
					assert.Equal(t, 1, timesCalled, "Message wasn't published.")
				}
			}
		}
	}
}

func TestPublishMultipleTimes(t *testing.T) {
	timesCalled := 0

	conn, err := NewConnection("amqp://guest:guest@broker:5672/")

	if assert.Nil(t, err) {
		defer conn.Close()

		channel, err := conn.openChannel()

		if assert.Nil(t, err) {
			defer channel.Close()

			c, err := NewConsumer(conn, false, "webhooks", "TestPublishMultipleTimes")

			if assert.Nil(t, err) {
				defer c.Close()

				// Clean all messages if any...
				channel.QueuePurge(c.queueName, false)

				c.Subscribe("action.name", func(e messaging.Event) error {
					defer func() { timesCalled++ }()
					return nil
				}, nil)

				go c.Consume()

				// take a time to setup topology
				time.Sleep(SleepSetupTopology)

				p, err := NewProducer(conn, "webhooks")

				if assert.Nil(t, err) {
					for i := 0; i < 5; i++ {
						input := messaging.MessageInput{
							Action:         "action.name",
							Data:           []byte(""),
							MessageGroupID: nil,
						}
						p.Publish(input)
					}

					time.Sleep(1 * time.Second)
					assert.Equal(t, 5, timesCalled, "One or more messages weren't published.")
				}
			}
		}
	}
}
