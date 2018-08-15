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
		c.VisibilityTimeout = 20
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
	config              *ConsumerConfig
	receiveMessageInput *sqs.ReceiveMessageInput
	m                   sync.RWMutex
	handlers            map[string]handler
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
		sqs:      sqs.New(sess),
		config:   config,
		stop:     make(chan bool),
		handlers: make(map[string]handler),
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

	log := logrus.WithFields(logrus.Fields{
		"action":     sns.TopicArn,
		"message_id": message.MessageId,
		"body":       sns.Message,
	})

	handler, ok := c.getHandler(sns.TopicArn)

	if !ok {
		log.WithError(err).Error("Action not found.")
		return
	}

	err = c.callAndHandlePanic(messaging.Event{
		Id:        *message.MessageId,
		Action:    handler.action,
		Body:      []byte(sns.Message),
		Timestamp: stringToTime(*message.Attributes["SentTimestamp"]),
	}, handler.fn)

	if err != nil {
		log.WithError(err).Debug("Failed to process event.")
		return
	}

	_, err = c.sqs.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.config.QueueUrl),
		ReceiptHandle: message.ReceiptHandle,
	})

	if err != nil {
		log.WithError(err).Error("Failed to delete message.")
		return
	}

	log.Debug("Message handled successfully.")
}

func (c *consumer) doConsume() {
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

func (c *consumer) Close() {
	c.stop <- true
}
