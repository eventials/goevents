package mock

import (
	"github.com/stretchr/testify/mock"
)

type Producer struct {
	mock.Mock
}

func (p *Producer) Publish(action string, data []byte) {
	args := p.Called(action, data)
}

func (p *Producer) Close() {
	args := p.Called()
}
