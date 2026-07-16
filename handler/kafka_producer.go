package handler

import (
	"context"
	"encoding/json"
	"fmt"

	trpckafka "trpc.group/trpc-go/trpc-database/kafka"
)

type KafkaProducer struct {
	client trpckafka.Client
}

func NewKafkaProducer() *KafkaProducer {
	return &KafkaProducer{client: trpckafka.NewClientProxy("trpc.kafka.producer.service")}
}

func (p *KafkaProducer) Send(ctx context.Context, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode Kafka message: %w", err)
	}
	if err := p.client.Produce(ctx, []byte(key), data); err != nil {
		return fmt.Errorf("publish Kafka message: %w", err)
	}
	return nil
}
