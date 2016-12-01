package events

type Producer interface {
	Publish(action string, data []byte) error
}
