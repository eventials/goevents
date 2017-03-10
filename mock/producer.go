package mock

import (
	"github.com/stretchr/testify/mock"
)

type Producer struct {
	mock.Mock
}

func (p *Producer) Publish(action string, data []byte) {
	p.Called(action, data)
}

func (p *Producer) Close() {
	p.Called()
}

func (p *Producer) NotifyClose() <-chan bool {
	p.Called()
	return make(chan bool)
}
