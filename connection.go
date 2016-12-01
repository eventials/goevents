package events

type Connection interface {
	Consumer(autoAck bool) (Consumer, error)
	Producer() (Producer, error)
	Close()
}
