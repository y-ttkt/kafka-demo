package kafkaclient

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

const (
	bootstrapServers = "127.0.0.1:29092"
	clientID         = "basic-producer-1"
)

func NewProducer() (*kafka.Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"client.id":         clientID,
	})

	if err != nil {
		return nil, err
	}

	return p, nil
}
