package events

type EventHandler func(body []byte) bool

type Consumer interface {
	Subscribe(action string, handler EventHandler) error
	Unsubscribe(action string) error
	Listen() error
	ListenForever() error
}
