package segmentio

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
)

type consumer struct {
	*kafka.Reader
}

func NewConsumer(config kafka.ReaderConfig) *consumer {
	return &consumer{kafka.NewReader(config)}
}

func (c *consumer) Subscribe() {
	for {
		m, err := c.Reader.ReadMessage(context.TODO())
		if err != nil {
			ErrChan <- err
		}
		SubChan <- m
	}
}

func (c *consumer) Close() {
	if c.Reader != nil {
		log.Println("close kafka reader ...")
		if err := c.Reader.Close(); err != nil {
			ErrChan <- errors.New(fmt.Sprintf("failed to close writer:%+v", err))
		}
	}
}
