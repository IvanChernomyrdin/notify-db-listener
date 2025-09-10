package listener

import (
	"context"
	"fmt"
	"sync"

	"db_listener/internal/config"

	"github.com/segmentio/kafka-go"
)

var (
	writer     *kafka.Writer
	writerOnce sync.Once
)

func SendToKafka(ctx context.Context, cfg *config.Config, value []byte) error {
	// ленивое создание writer для переиспользования
	writerOnce.Do(func() {
		writer = kafka.NewWriter(kafka.WriterConfig{
			Brokers:  cfg.Kafka.Brokers,
			Topic:    cfg.Kafka.Topic,
			Balancer: &kafka.LeastBytes{},
		})
	})

	// отправка сообщения
	err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte("email_json"),
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("failed to send data in kafka: %w", err)
	}

	return nil
}
