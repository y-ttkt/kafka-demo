package main

import (
	"context"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/y-ttkt/kafka-demo/internal/kafkaclient"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const topic = "ticket-order"

func main() {
	logger := log.New(os.Stdout, "[consumer] ", log.LstdFlags|log.Lmicroseconds)
	logger.Println("Start to poll records from Topic=", topic)

	consumer, err := kafkaclient.NewConsumer()
	if err != nil {
		logger.Fatalf("failed to create consumer: %v", err)
	}

	defer func() {
		logger.Println("Closing consumer...")
		_ = consumer.Close()
	}()

	if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
		logger.Fatalf("subscribe failed: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	for {
		select {
		case <-ctx.Done():
			logger.Println("shutdown signal received")
			return
		default:
			msg, err := consumer.ReadMessage(1 * time.Second)
			if err != nil {
				// タイムアウトは通常動作（新着無し）
				var kerr kafka.Error
				if errors.As(err, &kerr) && kerr.Code() == kafka.ErrTimedOut {
					continue
				}

				logger.Printf("unexpected error while polling: %v", err)
				continue
			}

			logger.Printf(
				"Received record: Partition=%d, Key=%s, Value=%s, Offset=%v",
				msg.TopicPartition.Partition,
				string(msg.Key),
				string(msg.Value),
				msg.TopicPartition.Offset,
			)
		}
	}
}
