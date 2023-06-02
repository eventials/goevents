package sns

import (
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/eventials/goevents/messaging"
	"github.com/sirupsen/logrus"
)

var (
	ErrEmptyConfig    = errors.New("empty config")
	ErrEmptyAccessKey = errors.New("empty access key")
	ErrEmptySecretKey = errors.New("empty secret key")
)

type snsMessagePayload struct {
	Message          string    `json:"Message"`
	MessageID        string    `json:"MessageId"`
	Signature        string    `json:"Signature"`
	SignatureVersion string    `json:"SignatureVersion"`
	SigningCertURL   string    `json:"SigningCertURL"`
	Subject          string    `json:"Subject"`
	Timestamp        time.Time `json:"Timestamp"`
	TopicArn         string    `json:"TopicArn"`
	Type             string    `json:"Type"`
	UnsubscribeURL   string    `json:"UnsubscribeURL"`
}

type ConsumerConfig struct {
	AccessKey           string
	SecretKey           string
	Region              string
	VisibilityTimeout   int64
	WaitTimeSeconds     int64
	MaxNumberOfMessages int64
	QueueUrl            string
}

func (c *ConsumerConfig) setDefaults() {
	if c.VisibilityTimeout == 0 {
		c.VisibilityTimeout = 45
	}

	if c.WaitTimeSeconds == 0 {
		c.WaitTimeSeconds = 20
	}

	if c.MaxNumberOfMessages == 0 {
		c.MaxNumberOfMessages = 5
	}
}

func (c *ConsumerConfig) isValid() error {
	if c == nil {
		return ErrEmptyConfig
	}

	if c.AccessKey == "" {
		return ErrEmptyAccessKey
	}

	if c.SecretKey == "" {
		return ErrEmptySecretKey
	}

	return nil
}

type handler struct {
	action  string
	fn      messaging.EventHandler
	options *messaging.SubscribeOptions
}

type consumer struct {
	sqs                 *sqs.SQS
	stop                chan bool
	qos                 chan bool
	config              *ConsumerConfig
	receiveMessageInput *sqs.ReceiveMessageInput
	m                   sync.RWMutex
	wg                  sync.WaitGroup
	handlers            map[string]handler
	processingMessages  map[string]bool
	mProcessingMessages sync.RWMutex
	closeOnce           sync.Once
	stopped             bool
}

func NewConsumer(config *ConsumerConfig) (messaging.Consumer, error) {
	if err := config.isValid(); err != nil {
		return nil, err
	}

	config.setDefaults()

	creds := credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, "")

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(config.Region),
		Credentials: creds,
	})

	if err != nil {
		return nil, err
	}

	c := &consumer{
		sqs:                sqs.New(sess),
		config:             config,
		stop:               make(chan bool),
		qos:                make(chan bool, config.MaxNumberOfMessages),
		handlers:           make(map[string]handler),
		processingMessages: make(map[string]bool),
	}

	c.receiveMessageInput = &sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            aws.String(c.config.QueueUrl),
		MaxNumberOfMessages: aws.Int64(c.config.MaxNumberOfMessages),
		VisibilityTimeout:   aws.Int64(c.config.VisibilityTimeout),
		WaitTimeSeconds:     aws.Int64(c.config.WaitTimeSeconds),
	}

	return c, nil
}

func MustNewConsumer(config *ConsumerConfig) messaging.Consumer {
	consumer, err := NewConsumer(config)

	if err != nil {
		panic(err)
	}

	return consumer
}

func (c *consumer) getHandler(action string) (handler, bool) {
	c.m.RLock()
	defer c.m.RUnlock()

	hnd, ok := c.handlers[action]

	return hnd, ok
}

// Subscribe subscribes an handler to a action. Action must be ARN URL.
func (c *consumer) Subscribe(action string, handlerFn messaging.EventHandler, options *messaging.SubscribeOptions) error {
	c.m.Lock()
	defer c.m.Unlock()

	c.handlers[action] = handler{
		action:  action,
		options: options,
		fn:      handlerFn,
	}

	return nil
}

func (c *consumer) Unsubscribe(action string) error {
	c.m.Lock()
	defer c.m.Unlock()

	delete(c.handlers, action)

	return nil
}

// TODO: needs to bind SNS Topic (actions) to SQS Queue in AWS.
func (c *consumer) BindActions(actions ...string) error {
	return nil
}

// TODO: reverts anything done by BindActions
func (c *consumer) UnbindActions(actions ...string) error {
	c.m.Lock()
	defer c.m.Unlock()

	for _, action := range actions {
		delete(c.handlers, action)
	}

	return nil
}

func stringToTime(t string) time.Time {
	if t == "" {
		return time.Time{}
	}

	i, err := strconv.ParseInt(t, 10, 64)

	if err != nil {
		return time.Time{}
	}

	return time.Unix(0, i*1000000)
}

func (c *consumer) callAndHandlePanic(event messaging.Event, fn messaging.EventHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("Unknown panic")
			}
		}
	}()

	err = fn(event)

	return
}

func (c *consumer) isMessageProcessing(id string) bool {
	c.mProcessingMessages.RLock()
	defer c.mProcessingMessages.RUnlock()

	_, ok := c.processingMessages[id]
	return ok
}

func (c *consumer) addMessageProcessing(id string) {
	c.mProcessingMessages.Lock()
	defer c.mProcessingMessages.Unlock()

	c.processingMessages[id] = true
}

func (c *consumer) deleteMessageProcessing(id string) {
	c.mProcessingMessages.Lock()
	defer c.mProcessingMessages.Unlock()

	delete(c.processingMessages, id)
}

func (c *consumer) handleMessage(message *sqs.Message) {
	sns := &snsMessagePayload{}
	err := json.Unmarshal([]byte(*message.Body), sns)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error": err,
			"body":  []byte(*message.Body),
		}).Error("Failed to unmarshall sns message.")
		return
	}

	id := *message.MessageId
	receiptHandle := *message.ReceiptHandle

	log := logrus.WithFields(logrus.Fields{
		"action":     sns.TopicArn,
		"message_id": id,
		"body":       sns.Message,
	})

	handler, ok := c.getHandler(sns.TopicArn)

	if !ok {
		log.Error("Action not found.")
		return
	}

	// Check if message is already processing in goroutine.
	// This will occur if consumer is slower than VisibilityTimeout.
	if c.isMessageProcessing(id) {
		log.Debug("Message is already processing.")
		return
	}

	// QOS: do not consume while prior messages are in goroutines.
	c.qos <- true

	c.addMessageProcessing(id)

	c.wg.Add(1)
	go func(event messaging.Event, fn messaging.EventHandler, receiptHandle string) {
		defer func() {
			c.wg.Done()

			c.deleteMessageProcessing(event.Id)

			<-c.qos
		}()

		err := c.callAndHandlePanic(event, fn)

		if err != nil {
			log.WithError(err).Debug("Failed to process event.")
			return
		}

		log.Debug("Deleting message.")

		_, err = c.sqs.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      aws.String(c.config.QueueUrl),
			ReceiptHandle: aws.String(receiptHandle),
		})

		if err != nil {
			log.WithError(err).Error("Failed to delete message.")
			return
		}

		log.Debug("Message handled successfully.")
	}(messaging.Event{
		Id:        id,
		Action:    handler.action,
		Body:      []byte(sns.Message),
		Timestamp: stringToTime(*message.Attributes["SentTimestamp"]),
	}, handler.fn, receiptHandle)
}

func (c *consumer) doConsume() {
	var nextMessages int64 = c.config.MaxNumberOfMessages - int64(len(c.qos))

	if nextMessages == 0 {
		logrus.Debugf("QOS full with %d.", c.config.MaxNumberOfMessages)
		time.Sleep(5 * time.Second)
		return
	}

	c.receiveMessageInput.MaxNumberOfMessages = aws.Int64(nextMessages)

	result, err := c.sqs.ReceiveMessage(c.receiveMessageInput)

	if err != nil {
		logrus.WithError(err).Error("Failed to get messages.")
		return
	}

	for _, message := range result.Messages {
		c.handleMessage(message)
	}
}

// Consume consumes with long-poll, messages from AWS SQS and dispatch to handlers.
// Polling time is configured by WaitTimeSeconds.
// Messages successfully handled will be deleted from SQS.
// Messages who failed to delete from SQS will be received again, and application needs to handle
// by using MessageId.
// Receiving duplicate messages may happen using more than one consumer if not processing in VisibilityTimeout.
func (c *consumer) Consume() {
	logrus.Info("Registered handlers:")

	for _, handler := range c.handlers {
		logrus.Infof("  %s", handler.action)
	}

	logrus.WithFields(logrus.Fields{
		"queue": c.config.QueueUrl,
	}).Info("Consuming messages...")

	for {
		select {
		case <-c.stop:
			return
		default:
			c.doConsume()
		}
	}
}

// PriorityConsume function takes a list of consumer objects and starts consuming them in a loop.
// The consumers are processed according to their priority. The process is stopped if the stop signal is received.
// The function exits when all consumers are stopped.
func PriorityConsume(consumers []*consumer) {
	logrus.Info("Registered handlers:")

	// Logging the registered handlers for each consumer
	for priority, consumer := range consumers {
		for _, handler := range consumer.handlers {
			logrus.Infof("  %s (priority %d)", handler.action, priority)
		}
	}

	// Counter for tracking the number of stopped consumers
	var consumersStopped int

	// Main loop to consume messages
	for {
		// Iterate over each consumer
		for _, consumer := range consumers {
			// Determine if a consumer should consume messages
			shouldConsume := !consumer.stopped && checkPriorityMessages(consumers, consumer)

			if !shouldConsume {
				continue
			}

			// If the consumer is still active, try to consume a message
			select {
			case <-consumer.stop:
				// If a stop signal is received, stop the consumer and increase the count of stopped consumers
				consumer.stopped = true
				consumersStopped++
			default:
				// If no stop signal, consume the message
				consumer.doConsume()
			}
		}

		// If all consumers are stopped, exit the function
		if consumersStopped == len(consumers) {
			return
		}
	}
}

// checkPriorityMessages checks whether the given consumer can consume a message.
// It checks all other consumers and allows a consumer to consume only if there are no higher priority consumers with messages to consume.
func checkPriorityMessages(consumers []*consumer, currentConsumer *consumer) bool {
	// Iterate over each consumer
	for _, consumer := range consumers {

		// Skip stopped consumers
		if consumer.stopped {
			continue
		}

		// If we reached the currentConsumer in the iteration, then all higher priority consumers were checked and they don't have any messages to consume.
		// So, the currentConsumer is allowed to consume.
		if consumer == currentConsumer {
			return true
		}

		// If any higher priority consumer has messages to consume, then the currentConsumer should not consume.
		if len(consumer.qos) > 0 {
			return false
		}
	}

	// If no consumers were found to have messages, the currentConsumer can consume.
	return true
}

func (c *consumer) doClose() {
	logrus.Info("Closing SNS consumer...")

	c.stop <- true

	logrus.Info("SNS consumer closed. Waiting remaining handlers...")
	c.wg.Wait()

	logrus.Info("SNS consumer closed.")

	close(c.stop)
}

func (c *consumer) Close() {
	c.closeOnce.Do(c.doClose)
}
