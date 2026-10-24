// Package event mengimplementasikan outbound.IEventProducer via Kafka.
package event

import (
	"context"
	"log/slog"

	"github.com/refreshTechRusuh/booking-service/pkg/kafka"
)

// KafkaProducer mengimplementasikan outbound.IEventProducer.
type KafkaProducer struct {
	p   *kafka.Producer
	log *slog.Logger
}

func NewKafkaProducer(p *kafka.Producer, log *slog.Logger) *KafkaProducer {
	return &KafkaProducer{p: p, log: log}
}

func (k *KafkaProducer) PublishBookingCreated(ctx context.Context, eventID string, payload any) error {
	return k.p.Publish(ctx, kafka.TopicBookingCreated, eventID, payload)
}

func (k *KafkaProducer) PublishStockChanged(ctx context.Context, eventID string, payload any) error {
	return k.p.Publish(ctx, kafka.TopicStockChanged, eventID, payload)
}

func (k *KafkaProducer) PublishSlotReleased(ctx context.Context, eventID string, payload any) error {
	return k.p.Publish(ctx, kafka.TopicSlotReleased, eventID, payload)
}

func (k *KafkaProducer) PublishEventSoldOut(ctx context.Context, eventID string, payload any) error {
	return k.p.Publish(ctx, kafka.TopicEventSoldOut, eventID, payload)
}
