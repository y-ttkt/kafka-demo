package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/y-ttkt/kafka-demo/internal/kafkaclient"
	"log"
	"os"
)

const topic = "ticket-order"

func main() {
	logger := log.New(os.Stdout, "[producer] ", log.LstdFlags|log.Lmicroseconds)
	logger.Println("Producer started.")

	orderID := uuid.New().String()
	userID := "123"
	contentID := "55555"
	eventValue := fmt.Sprintf(
		"order_id=%s, user_id=%s, content_id=%s",
		orderID, userID, contentID,
	)

	producer, err := kafkaclient.NewProducer()
	if err != nil {
		logger.Printf("failed to create producer: %v", err)
	}

	defer func() {
		// 未送信（送信中・キューに残っている）メッセージが0になる
		// もしくは timeoutMs ミリ秒経過するまで待つ
		remaining := producer.Flush(10 * 1000)
		if remaining > 0 {
			log.Printf("Failed to flush %d messages", remaining)
			// リトライする / エラーとして扱う / アラート出す…などは別途対応
		}
		producer.Close()
		logger.Println("Producer shutdown...")
	}()

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &[]string{topic}[0],
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(orderID),
		Value: []byte(eventValue),
	}

	deliveryChan := make(chan kafka.Event, 1)

	if err := producer.Produce(msg, deliveryChan); err != nil {
		logger.Printf("failed to produce message: %v", err)
	}

	e := <-deliveryChan
	m, ok := e.(*kafka.Message)
	if !ok {
		logger.Fatalf("unexpected event type: %T", e)
	}
	close(deliveryChan)

	if m.TopicPartition.Error != nil {
		logger.Fatalf("delivery failed: %v", m.TopicPartition.Error)
	} else {
		logger.Printf("delivered to %s [%d] at offset %v",
			*m.TopicPartition.Topic,
			m.TopicPartition.Partition,
			m.TopicPartition.Offset,
		)
	}
}
