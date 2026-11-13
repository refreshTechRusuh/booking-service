// Package kafka menyediakan Kafka producer/consumer wrapper via segmentio/kafka-go.
// Mendukung SASL/TLS untuk Confluent managed (production).
package kafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/refreshTechRusuh/booking-service/internal/config"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// ---- Producer ---------------------------------------------------------------

// Producer menulis pesan ke satu atau lebih topic Kafka.
type Producer struct {
	writer *kafkago.Writer
	log    *slog.Logger
}

// NewProducer membuat Producer yang terhubung ke broker.
func NewProducer(cfg config.KafkaConfig, log *slog.Logger) *Producer {
	transport := &kafkago.Transport{}
	if cfg.TLS || cfg.Username != "" {
		transport.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if cfg.Username != "" {
		transport.SASL = plain.Mechanism{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}

	w := &kafkago.Writer{
		Addr:         kafkago.TCP(strings.Split(cfg.Brokers, ",")...),
		Balancer:     &kafkago.Hash{}, // partition by key (eventId)
		RequiredAcks: kafkago.RequireAll,
		Async:        false,
		Transport:    transport,
	}

	return &Producer{writer: w, log: log}
}

// Publish mengirim satu pesan ke topic dengan key sebagai partition key.
func (p *Producer) Publish(ctx context.Context, topic string, key string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("kafka producer: marshal: %w", err)
	}

	msg := kafkago.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: b,
		Time:  time.Now(),
	}

	// Buka span kafka.produce dan inject traceparent ke message header.
	ctx, span := StartProduceSpan(ctx, topic)
	defer span.End()
	InjectTraceContext(ctx, &msg)

	start := time.Now()
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		span.RecordError(err)
		p.log.ErrorContext(ctx, "kafka publish failed",
			slog.String("topic", topic),
			slog.String("key", key),
			slog.String("duration", time.Since(start).String()),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("kafka producer: publish %s: %w", topic, err)
	}

	p.log.InfoContext(ctx, "kafka publish ok",
		slog.String("topic", topic),
		slog.String("key", key),
		slog.Int("bytes", len(b)),
		slog.String("duration", time.Since(start).String()),
	)
	return nil
}

// Close menutup koneksi writer.
func (p *Producer) Close() error { return p.writer.Close() }

// ---- Topic constants --------------------------------------------------------

const (
	TopicBookingCreated   = "booking.created"
	TopicStockChanged     = "stock.changed"
	TopicSlotReleased     = "slot.released"
	TopicPaymentSucceeded = "payment.succeeded"
	TopicPaymentExpired   = "payment.expired"
	TopicEventSoldOut     = "event.sold_out"
)

// ---- Event payloads ---------------------------------------------------------

type BookingCreatedEvent struct {
	BookingID        string             `json:"bookingId"`
	EventID          string             `json:"eventId"`
	EventName        string             `json:"eventName"`        // nama event untuk email #1
	Email            string             `json:"email"`
	Items            []BookingItemEvent `json:"items"`
	TotalAmount      int64              `json:"totalAmount"`
	PaymentExpiresAt string             `json:"paymentExpiresAt,omitempty"` // RFC3339; deadline bayar untuk email #1
}

type BookingItemEvent struct {
	CategoryID   string `json:"categoryId"`
	CategoryName string `json:"categoryName"` // nama kategori untuk email #1
	NIK          string `json:"nik"`
	Name         string `json:"name"`         // nama penonton dari input (untuk email #1)
	Price        int64  `json:"price"`
}

type StockChangedEvent struct {
	EventID    string `json:"eventId"`
	CategoryID string `json:"categoryId"`
	Delta      int    `json:"delta"`     // negatif = hold, positif = release
	Available  int    `json:"available"` // nilai available setelah perubahan
	Reason     string `json:"reason"`    // HOLD | RELEASE | PAID
}

type SlotReleasedEvent struct {
	BookingID string `json:"bookingId"`
	EventID   string `json:"eventId"`
	Reason    string `json:"reason"` // PAID | BOOKING_TTL | PAYMENT_TTL | CANCELLED
	// QueueToken (opsional): bila ada, queue-service melepas kursi token itu lebih awal.
	QueueToken string `json:"queueToken,omitempty"`
}

type PaymentSucceededEvent struct {
	PaymentID string             `json:"paymentId"`
	BookingID string             `json:"bookingId"`
	EventID   string             `json:"eventId"`
	Method    string             `json:"method"`
	Items     []BookingItemEvent `json:"items"`
}

type PaymentExpiredEvent struct {
	PaymentID string `json:"paymentId"`
	BookingID string `json:"bookingId"`
	EventID   string `json:"eventId"`
}

type EventSoldOutEvent struct {
	EventID string `json:"eventId"`
}
