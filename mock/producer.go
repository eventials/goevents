package mock

import (
	"github.com/eventials/goevents/messaging"
	"github.com/stretchr/testify/mock"
)

type Producer struct {
	mock.Mock
}

func NewMockProducer() messaging.Producer {
	return &Producer{}
}

func (p *Producer) Publish(input messaging.MessageInput) {
	p.Called(input.Action, input.Data, input.MessageGroupID)
}

func (p *Producer) Close() {
	p.Called()
}

func (p *Producer) NotifyClose() <-chan bool {
	args := p.Called()
	return args.Get(0).(chan bool)
}
