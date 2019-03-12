package messaging

import (
	"context"
	"time"
)

type Event struct {
	Id        string
	Action    string
	Timestamp time.Time
	Body      []byte
	Ack       func(multiple bool) error
	Nack      func(multiple, requeue bool) error
	Reject    func(requeue bool) error
	ctx       context.Context
}

// WithContext returns a shallow copy of Event with its context changed to ctx.
// The provided ctx must be non-nil.
func (e Event) WithContext(ctx context.Context) Event {
	if ctx == nil {
		panic("nil context")
	}

	e.ctx = ctx

	return e
}

// Context returns the current context; if nil, it defaults to the background context.
// To change the context, use WithContext.
func (e Event) Context() context.Context {
	if e.ctx != nil {
		return e.ctx
	}

	return context.Background()
}
