package messaging

type Producer interface {
	Publish(action string, data []byte)
	PublishToQueue(queue string, data []byte)
	NotifyClose() <-chan bool
	Close()
}
