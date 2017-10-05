package messaging

import (
	"context"
	"time"
)

const (
	MaxInt32   = 1<<31 - 1
	MaxRetries = MaxInt32
)

type SubscribeOptions struct {
	// The time to retry after it fails.
	RetryDelay time.Duration
	// If enable the retry time it will be incresed in power of two.
	// This means if your retry delay is 1s, the first retry will be after 1s,
	// the sencond 2s, the third 4s and so on.
	DelayedRetry bool
	// Max attempts to retry.
	MaxRetries int32
}

type Event struct {
	Action  string
	Body    []byte
	Context context.Context
}

type EventHandler func(Event) error

type Consumer interface {
	Subscribe(action string, handler EventHandler, options *SubscribeOptions) error
	Unsubscribe(action string) error
	Consume()
	Close()
}
