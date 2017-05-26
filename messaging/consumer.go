package messaging

import (
	"time"
)

type EventHandler func(body []byte) error

type Consumer interface {
	Subscribe(action string, handler EventHandler) error
	SubscribeWithOptions(action string, handlerFn EventHandler,
		retryDelay time.Duration, delayPow2 bool, maxRetries int32) error
	Unsubscribe(action string) error
	Consume()
	Close()
}
