package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/y-ttkt/kafka-demo/internal/kafkaclient"
	"log"
	"os"
	"time"
)

const topic = "ticket-order"

func main() {
	logger := log.New(os.Stdout, "[producer] ", log.LstdFlags|log.Lmicroseconds)
	logger.Println("Producer started.")

	producer, err := kafkaclient.NewTransactionalProducer()
	if err != nil {
		logger.Fatalf("failed to create producer: %v", err)
	}

	defer func() {
		remaining := producer.Flush(10 * 1000)
		if remaining > 0 {
			log.Printf("Failed to flush %d messages", remaining)
		}
		producer.Close()
		logger.Println("Producer shutdown...")
	}()

	ctxInit, cancelInit := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelInit()

	if err := producer.InitTransactions(ctxInit); err != nil {
		logger.Fatalf("failed to init transactions: %v", err)
	}

	orderID := uuid.NewString()
	value := fmt.Sprintf("order_id=%s, user_id=%s, content_id=%s", orderID, "123", "55555")

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &[]string{topic}[0],
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(orderID),
		Value: []byte(value),
	}

	if err := producer.BeginTransaction(); err != nil {
		logger.Fatalf("BeginTransaction failed: %v", err)
	}

	deliveryChan := make(chan kafka.Event, 1)
	if err := producer.Produce(msg, deliveryChan); err != nil {
		logger.Printf("Produce failed before send: %v", err)
		abortTx(producer, logger)
		return
	}

	e := <-deliveryChan
	m, ok := e.(*kafka.Message)
	close(deliveryChan)

	if !ok {
		logger.Printf("unexpected event type: %T", e)
		abortTx(producer, logger)
		return
	}

	if m.TopicPartition.Error != nil {
		logger.Printf("delivery failed: %v", m.TopicPartition.Error)
		abortTx(producer, logger)
		return
	}
	logger.Printf("sent record: topic=%s partition=%d offset=%v",
		*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)

	ctxCommit, cancelCommit := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelCommit()
	if err := producer.CommitTransaction(ctxCommit); err != nil {
		logger.Printf("CommitTransaction failed: %v", err)
		abortTx(producer, logger)
		return
	}
	logger.Println("transaction committed")
}

func abortTx(producer *kafka.Producer, logger *log.Logger) {
	ctxAborted, cancelAborted := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelAborted()
	if err := producer.AbortTransaction(ctxAborted); err != nil {
		logger.Printf("AbortTransaction failed: %v", err)
	} else {
		logger.Println("AbortTransaction succeeded")
	}
}
