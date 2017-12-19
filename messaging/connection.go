package messaging

type Connection interface {
	Consumer(autoAck bool, exchange, queue string) (Consumer, error)
	Producer(exchange string) (Producer, error)
	Close()
	NotifyConnectionClose() <-chan error
	NotifyReestablish() <-chan bool
	WaitUntilConnectionCloses()
	IsConnected() bool
}
