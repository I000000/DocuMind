package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

// Producer — обёртка над SyncProducer с трассировкой и JSON-сериализацией.
type Producer struct {
	producer sarama.SyncProducer
	topic    string
	logger   *zap.Logger
}

// NewProducer создаёт SyncProducer.
func NewProducer(brokers []string, topic string, logger *zap.Logger) (*Producer, error) {
	cfg := sarama.NewConfig()

	// acks=all — ждём подтверждения от всех in-sync реплик.
	cfg.Producer.RequiredAcks = sarama.WaitForAll

	// Идемпотентность: producer сам избегает дубликатов при retry.
	cfg.Producer.Idempotent = true
	cfg.Net.MaxOpenRequests = 1

	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Timeout = 10 * time.Second

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("create sync producer: %w", err)
	}

	logger.Info("kafka_producer_started",
		zap.Strings("brokers", brokers),
		zap.String("topic", topic),
	)

	return &Producer{
		producer: producer,
		topic:    topic,
		logger:   logger,
	}, nil
}

// Publish отправляет событие в Kafka с прокидыванием trace context.
// Возвращает partition и offset для логов.
func (p *Producer) Publish(
	ctx context.Context,
	key string,
	value any,
	headers map[string]string,
) (int32, int64, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return 0, 0, fmt.Errorf("marshal payload: %w", err)
	}

	// Прокидываем traceparent через Kafka headers
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(payload),
	}

	for k, v := range carrier {
		msg.Headers = append(msg.Headers, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}
	for k, v := range headers {
		msg.Headers = append(msg.Headers, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return 0, 0, fmt.Errorf("send message: %w", err)
	}

	p.logger.Info("kafka_published",
		zap.String("topic", p.topic),
		zap.String("key", key),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
	)
	return partition, offset, nil
}

// Close корректно закрывает producer, дожидаясь отправки буфера.
func (p *Producer) Close() error {
	if err := p.producer.Close(); err != nil {
		return fmt.Errorf("close producer: %w", err)
	}
	p.logger.Info("kafka_producer_stopped")
	return nil
}
