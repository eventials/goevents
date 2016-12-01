package mock

import (
	"github.com/stretchr/testify/mock"
)

type Producer struct {
	mock.Mock
}

func (p *Producer) Publish(action string, data []byte) error {
	args := p.Called(action, data)
	return args.Error(1)
}
