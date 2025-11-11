package kafkaclient

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

const (
	txBootstrapServers = "broker-1:9092,broker-2:9092,broker-3:9092"
	txClientID         = "basic-producer-tx-1"
	// とりあえず単一プロセス前提
	transactionalID = "sample-tx-id"
)

func NewTransactionalProducer() (*kafka.Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  txBootstrapServers,
		"client.id":          txClientID,
		"acks":               "all",
		"enable.idempotence": true,
		"retries":            5,
		"transactional.id":   transactionalID,
	})

	if err != nil {
		return nil, err
	}

	return p, nil
}
