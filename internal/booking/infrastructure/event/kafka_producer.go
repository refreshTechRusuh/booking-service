// Package event mengimplementasikan outbound.IEventProducer via Kafka.
package event

import "context"

// Topic constants — duplikat dari pkg/kafka untuk menghindari import loop.
const (
	topicBookingCreated = "booking.created"
	topicStockChanged   = "stock.changed"
	topicSlotReleased   = "slot.released"
	topicEventSoldOut   = "event.sold_out"
)

// KafkaProducer mengimplementasikan outbound.IEventProducer.
type KafkaProducer struct {
	p IPublisher
}

// NewKafkaProducer membuat KafkaProducer dengan publisher yang diberikan.
// Di production, pass *kafka.Producer. Di test, pass mock.
func NewKafkaProducer(p IPublisher) *KafkaProducer {
	return &KafkaProducer{p: p}
}

func (k *KafkaProducer) PublishBookingCreated(ctx context.Context, eventID string, payload any) error {
	return k.p.Publish(ctx, topicBookingCreated, eventID, payload)
}

func (k *KafkaProducer) PublishStockChanged(ctx context.Context, eventID string, payload any) error {
	return k.p.Publish(ctx, topicStockChanged, eventID, payload)
}

func (k *KafkaProducer) PublishSlotReleased(ctx context.Context, eventID string, payload any) error {
	return k.p.Publish(ctx, topicSlotReleased, eventID, payload)
}

func (k *KafkaProducer) PublishEventSoldOut(ctx context.Context, eventID string, payload any) error {
	return k.p.Publish(ctx, topicEventSoldOut, eventID, payload)
}
