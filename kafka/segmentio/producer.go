package segmentio

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

type producer struct {
	*kafka.Writer
}

func NewProducer(config kafka.Writer) *producer {
	return &producer{&config}
}
func (c *producer) Send(msg []kafka.Message) {
	err := c.Writer.WriteMessages(context.TODO(), msg...)
	if err != nil {
		for i := 0; i < retries; i++ {
			ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
			defer cancel()

			// attempt to create topic prior to publishing the message
			err = c.Writer.WriteMessages(ctx, msg...)
			if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
				time.Sleep(time.Millisecond * 250)
				continue
			}

			if err != nil {
				ErrChan <- err
			}
			break
		}
	}
}

func (c *producer) Close() {
	if c.Writer != nil {
		log.Println("close kafka writer ...")
		if err := c.Writer.Close(); err != nil {
			ErrChan <- errors.New(fmt.Sprintf("failed to close writer:%+v", err))
		}
	}

}
