package event_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/refreshTechRusuh/booking-service/internal/booking/infrastructure/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(nil, &slog.HandlerOptions{Level: slog.LevelError + 10}))
}

// ── mockPublisher ─────────────────────────────────────────────────────────────

type mockPublisher struct {
	calls  []publishCall
	retErr error
}

type publishCall struct {
	topic   string
	key     string
	payload any
}

func (m *mockPublisher) Publish(_ context.Context, topic, key string, payload any) error {
	m.calls = append(m.calls, publishCall{topic: topic, key: key, payload: payload})
	return m.retErr
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestKafkaProducer_PublishBookingCreated(t *testing.T) {
	mp := &mockPublisher{}
	p := event.NewKafkaProducer(mp)

	err := p.PublishBookingCreated(context.Background(), "event-1", map[string]string{"key": "val"})
	require.NoError(t, err)
	require.Len(t, mp.calls, 1)
	assert.Equal(t, "booking.created", mp.calls[0].topic)
	assert.Equal(t, "event-1", mp.calls[0].key)
}

func TestKafkaProducer_PublishStockChanged(t *testing.T) {
	mp := &mockPublisher{}
	p := event.NewKafkaProducer(mp)

	err := p.PublishStockChanged(context.Background(), "event-1", map[string]any{"delta": -2})
	require.NoError(t, err)
	require.Len(t, mp.calls, 1)
	assert.Equal(t, "stock.changed", mp.calls[0].topic)
	assert.Equal(t, "event-1", mp.calls[0].key)
}

func TestKafkaProducer_PublishSlotReleased(t *testing.T) {
	mp := &mockPublisher{}
	p := event.NewKafkaProducer(mp)

	err := p.PublishSlotReleased(context.Background(), "event-1", map[string]string{"reason": "EXPIRED"})
	require.NoError(t, err)
	require.Len(t, mp.calls, 1)
	assert.Equal(t, "slot.released", mp.calls[0].topic)
}

func TestKafkaProducer_PublishEventSoldOut(t *testing.T) {
	mp := &mockPublisher{}
	p := event.NewKafkaProducer(mp)

	err := p.PublishEventSoldOut(context.Background(), "event-1", map[string]string{"eventId": "event-1"})
	require.NoError(t, err)
	require.Len(t, mp.calls, 1)
	assert.Equal(t, "event.sold_out", mp.calls[0].topic)
}

func TestKafkaProducer_PublishError_propagated(t *testing.T) {
	mp := &mockPublisher{retErr: errors.New("kafka connection refused")}
	p := event.NewKafkaProducer(mp)

	err := p.PublishBookingCreated(context.Background(), "event-1", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kafka connection refused")
}

func TestKafkaProducer_AllTopicsSequential(t *testing.T) {
	mp := &mockPublisher{}
	p := event.NewKafkaProducer(mp)

	_ = p.PublishBookingCreated(context.Background(), "e1", nil)
	_ = p.PublishStockChanged(context.Background(), "e1", nil)
	_ = p.PublishSlotReleased(context.Background(), "e1", nil)
	_ = p.PublishEventSoldOut(context.Background(), "e1", nil)

	require.Len(t, mp.calls, 4)
	topics := []string{mp.calls[0].topic, mp.calls[1].topic, mp.calls[2].topic, mp.calls[3].topic}
	assert.ElementsMatch(t, []string{"booking.created", "stock.changed", "slot.released", "event.sold_out"}, topics)
}
