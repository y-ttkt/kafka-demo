package kafkaclient

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

const (
	consumerBootstrapServers = "broker:9092"
	consumerGroupID          = "G1"
)

func NewConsumer() (*kafka.Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": consumerBootstrapServers,
		"group.id":          consumerGroupID,
		"client.id":         uuid.New().String(),
		// GroupIDで未コミットのPartitionは先頭から読む
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		return nil, err
	}

	return c, nil
}
