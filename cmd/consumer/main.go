package main

import (
	"context"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/y-ttkt/kafka-demo/internal/kafkaclient"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const (
	topic          = "ticket-order"
	httpTargetURL  = "https://httpbin.org/post"
	httpTimeoutSec = 5
)

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

	httpClient := &http.Client{
		Timeout: httpTimeoutSec * time.Second,
	}

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

			partition := msg.TopicPartition.Partition
			offset := msg.TopicPartition.Offset
			key := string(msg.Key)
			val := string(msg.Value)

			logger.Printf("Received: Partition=%d Offset=%v Key=%s Value=%s",
				partition, offset, key, val,
			)

			go func(body string) {
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, httpTargetURL, strings.NewReader(body))
				if err != nil {
					logger.Printf("failed to create request: %v", err)
					return
				}
				req.Header.Set("Content-Type", "text/plain")

				resp, err := httpClient.Do(req)
				if err != nil {
					logger.Printf("failed to send request: %v", err)
					return
				}
				defer resp.Body.Close()

				respBody, err := io.ReadAll(resp.Body)
				if err != nil {
					logger.Printf("failed to read http response: %v", err)
					return
				}

				logger.Printf("HTTP response: %s", string(respBody))
			}(val)
		}
	}
}
