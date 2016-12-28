package messaging

type Connection interface {
	Consumer(autoAck bool, exchange, queue string) (Consumer, error)
	Producer(exchange, queue string) (Producer, error)
	Close()
	NotifyConnectionClose() <-chan error
	WaitUntilConnectionCloses()
}
