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
	ctx       context.Context
}

// WithContext returns a shallow copy of Event with its context changed to ctx.
// The provided ctx must be non-nil.
func (e *Event) WithContext(ctx context.Context) *Event {
	if ctx == nil {
		panic("nil context")
	}

	e2 := new(Event)
	*e2 = *e
	e2.ctx = ctx

	return e2
}

// The returned context is always non-nil; it defaults to the background context.
// To change the context, use WithContext.
func (e *Event) Context() context.Context {
	if e.ctx != nil {
		return e.ctx
	}

	return context.Background()
}
