package event

import "context"

// IPublisher adalah interface minimal untuk kafka producer.
// *kafka.Producer memenuhi interface ini di production.
// mockPublisher memenuhi interface ini di test.
type IPublisher interface {
	Publish(ctx context.Context, topic, key string, payload any) error
}
