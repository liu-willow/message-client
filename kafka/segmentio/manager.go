package segmentio

import (
	"github.com/segmentio/kafka-go"
	"log"
)

var (
	SubChan = make(chan kafka.Message)
	ErrChan = make(chan error)
)

type Manager struct {
	Writer *producer
	Reader *consumer
}

const retries = 3

func (s *Manager) Close() {
	if s.Writer != nil {
		s.Writer.Close()
	}
	if s.Reader != nil {
		s.Reader.Close()
	}
	ErrChan = nil
	SubChan = nil
	log.Println("client close!!!")
}
