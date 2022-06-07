package main

import (
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"message-client/kafka/segmentio"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	c := make(chan os.Signal, 1)
	// 监听信号
	signal.Notify(c, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	addr := []string{"192.168.31.150:9092"}

	client := segmentio.Manager{
		Writer: segmentio.NewProducer(kafka.Writer{
			Addr:     kafka.TCP(addr...),
			Balancer: &kafka.LeastBytes{},
			//允许发布消息时创建不存在的主题
			AllowAutoTopicCreation: true,
			//压缩消息
			Compression: kafka.Snappy,
		}),
		Reader: segmentio.NewConsumer(kafka.ReaderConfig{
			Brokers:     addr,
			GroupID:     "willow",
			GroupTopics: []string{"liu", "willow"},
			Partition:   0,
			MinBytes:    10e3, // 10KB
			MaxBytes:    10e6, // 10MB
		}),
	}

	ticker := time.NewTicker(time.Second * 1)

	counter := 1

	go func(tick *time.Ticker) {
		for range tick.C {
			client.Writer.Send([]kafka.Message{
				{
					Topic: "liu",
					Key:   []byte(fmt.Sprintf("liu-[%+v]", counter)),
					Value: []byte(fmt.Sprintf("你好,liu-%+v", counter)),
				},
				{
					Topic: "willow",
					Key:   []byte(fmt.Sprintf("willow-[%+v]", counter)),
					Value: []byte(fmt.Sprintf("你好,willow-%+v", counter)),
				},
			})
			counter++
			if counter >= 5 {
				tick.Stop()
			}
		}

	}(ticker)

	go client.Reader.Subscribe()

	for {
		select {
		case err := <-segmentio.ErrChan:
			log.Printf("recv error:[%+v]", err)
			break
		case sub := <-segmentio.SubChan:
			log.Printf("recv sub log:[%+v]", string(sub.Value))
			break
		case signal_c := <-c:
			log.Printf("recv sign:[%+v]", signal_c)
			client.Close()
			os.Exit(1)
		}
	}
}
