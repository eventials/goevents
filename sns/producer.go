package sns

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/eventials/goevents/messaging"
	"github.com/sirupsen/logrus"
)

type SendMessageType int

const (
	SendWithAction SendMessageType = iota
	SendWithQueue
)

type message struct {
	action      string
	queue       string
	messageType SendMessageType
	data        []byte
}

type ProducerConfig struct {
	AccessKey       string
	SecretKey       string
	Region          string
	PublishInterval time.Duration
}

func (p *ProducerConfig) setDefaults() {
	if p.PublishInterval == 0 {
		p.PublishInterval = 2 * time.Second
	}
}

func (p *ProducerConfig) isValid() error {
	if p == nil {
		return ErrEmptyConfig
	}

	if p.AccessKey == "" {
		return ErrEmptyAccessKey
	}

	if p.SecretKey == "" {
		return ErrEmptySecretKey
	}

	return nil
}

type producer struct {
	config        ProducerConfig
	m             sync.Mutex
	sns           *sns.SNS
	sqs           *sqs.SQS
	internalQueue chan message
	closes        []chan bool
	closed        bool
	closeOnce     sync.Once
}

func NewProducer(config ProducerConfig) (messaging.Producer, error) {
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

	p := &producer{
		sns:           sns.New(sess),
		sqs:           sqs.New(sess),
		internalQueue: make(chan message),
		config:        config,
		closed:        false,
	}

	go p.drainInternalQueue()

	return p, nil
}

func MustNewProducer(config ProducerConfig) messaging.Producer {
	p, err := NewProducer(config)

	if err != nil {
		panic(err)
	}

	return p
}

func (p *producer) Publish(action string, data []byte) {
	p.internalQueue <- message{
		action:      action,
		messageType: SendWithAction,
		data:        data,
	}

}

func (p *producer) PublishToQueue(queue string, data []byte) {
	p.internalQueue <- message{
		queue:       queue,
		messageType: SendWithQueue,
		data:        data,
	}
}

func (p *producer) isClosed() bool {
	p.m.Lock()
	defer p.m.Unlock()

	return p.closed
}

func (p *producer) drainInternalQueue() {
	for m := range p.internalQueue {
		retry := true

		for retry && !p.isClosed() {
			if m.messageType == SendWithAction {
				output, err := p.sns.Publish(&sns.PublishInput{
					Message:  aws.String(string(m.data)),
					TopicArn: aws.String(m.action),
				})

				if err != nil {
					logrus.WithFields(logrus.Fields{
						"message": m,
						"error":   err,
					}).Error("Failed to publish message.")

					time.Sleep(p.config.PublishInterval)
				} else {
					logrus.WithFields(logrus.Fields{
						"message_id": output.MessageId,
					}).Debug("Successfully published message.")

					retry = false
				}
			} else {
				output, err := p.sqs.SendMessage(&sqs.SendMessageInput{
					MessageBody:  aws.String(string(m.data)),
					QueueUrl:     aws.String(m.queue),
					DelaySeconds: aws.Int64(0),
				})

				if err != nil {
					logrus.WithFields(logrus.Fields{
						"message": m,
						"error":   err,
					}).Error("Failed to publish message.")

					time.Sleep(p.config.PublishInterval)
				} else {
					logrus.WithFields(logrus.Fields{
						"message_id": output.MessageId,
					}).Debug("Successfully published message.")

					retry = false
				}
			}
		}
	}

	p.m.Lock()
	for _, c := range p.closes {
		c <- true
	}
	p.m.Unlock()
}

func (p *producer) NotifyClose() <-chan bool {
	receiver := make(chan bool, 1)

	p.m.Lock()
	p.closes = append(p.closes, receiver)
	p.m.Unlock()

	return receiver
}

func (p *producer) doClose() {
	p.m.Lock()
	defer p.m.Unlock()

	p.closed = true

	close(p.internalQueue)
}

func (p *producer) Close() {
	p.closeOnce.Do(p.doClose)
}
