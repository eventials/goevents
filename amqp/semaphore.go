package amqp

type Semaphore struct {
	c chan bool
}

func NewSemaphore(size int) Semaphore {
	return Semaphore{
		c: make(chan bool, size),
	}
}

func (s *Semaphore) Acquire() {
	s.c <- true
}

func (s *Semaphore) Release() {
	<-s.c
}
