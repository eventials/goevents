package messaging

import (
	"time"
)

type EventHandler func(body []byte) error

type SubscribeOptions struct {
	// The action name.
	Action string
	// The function that will be called.
	Handler EventHandler
	// The time to retry after it fails.
	RetryDelay time.Duration
	// If enable the retry time it will be incresed in power of two.
	// This means if your retry delay is 1s, the first retry will be after 1s,
	// the sencond 2s, the third 4s and so on.
	DelayedRetry bool
	// Max attempts to retry.
	MaxRetries int32
}

type Consumer interface {
	Subscribe(action string, handler EventHandler) error
	SubscribeWithOptions(options SubscribeOptions) error
	Unsubscribe(action string) error
	Consume()
	Close()
}
