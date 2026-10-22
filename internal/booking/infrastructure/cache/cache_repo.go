// Package cache mengimplementasikan outbound.ICacheRepository via Valkey.
package cache

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/golang-jwt/jwt/v5"
	"github.com/refreshTechRusuh/booking-service/internal/booking/port/outbound"
	vk "github.com/valkey-io/valkey-go"
)

// CacheRepository mengimplementasikan outbound.ICacheRepository.
type CacheRepository struct {
	client vk.Client
	secret string // ACCESS_TOKEN_SECRET shared dengan queue-service
	log    *slog.Logger
}

func NewCacheRepository(client vk.Client, secret string, log *slog.Logger) *CacheRepository {
	return &CacheRepository{client: client, secret: secret, log: log}
}

// ValidateAccessToken memverifikasi JWT self-verifiable (tidak perlu roundtrip ke Redis).
func (r *CacheRepository) ValidateAccessToken(ctx context.Context, tokenStr string) (*outbound.AccessTokenClaims, error) {
	type accessClaims struct {
		EventID             string `json:"eventId"`
		QueueToken          string `json:"queueToken"`
		NIK                 string `json:"nik,omitempty"`
		BookingTTLExpiresAt int64  `json:"bookingTtlExpiresAt"`
		jwt.RegisteredClaims
	}

	claims := &accessClaims{}
	_, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, jwt.ErrSignatureInvalid
		}
		return []byte(r.secret), nil
	})
	if err != nil {
		return nil, fmt.Errorf("cache: validate access token: %w", err)
	}

	return &outbound.AccessTokenClaims{
		EventID:             claims.EventID,
		QueueToken:          claims.QueueToken,
		NIK:                 claims.NIK,
		BookingTTLExpiresAt: claims.BookingTTLExpiresAt,
	}, nil
}

// PublishStockChanged mempublish payload ke channel Redis Pub/Sub.
func (r *CacheRepository) PublishStockChanged(ctx context.Context, eventID string, payload []byte) error {
	channel := fmt.Sprintf("stock:%s", eventID)
	return r.client.Do(ctx,
		r.client.B().Publish().Channel(channel).Message(string(payload)).Build(),
	).Error()
}

// SubscribeStockChanged subscribe ke channel Redis Pub/Sub untuk SSE stok.
func (r *CacheRepository) SubscribeStockChanged(ctx context.Context, eventID string) (<-chan []byte, func(), error) {
	channel := fmt.Sprintf("stock:%s", eventID)
	ch := make(chan []byte, 32)

	// valkey-go: Dedicated Pub/Sub client.
	dedicatedClient, cancel := r.client.Dedicate()

	go func() {
		defer close(ch)

		if err := dedicatedClient.Do(ctx,
			dedicatedClient.B().Subscribe().Channel(channel).Build(),
		).Error(); err != nil {
			r.log.ErrorContext(ctx, "subscribe failed", slog.String("channel", channel), slog.Any("err", err))
			return
		}

		for {
			err := dedicatedClient.Receive(ctx,
				dedicatedClient.B().Subscribe().Channel(channel).Build(),
				func(m vk.PubSubMessage) {
					select {
					case ch <- []byte(m.Message):
					default:
						r.log.WarnContext(ctx, "stock SSE channel full, dropping", slog.String("eventId", eventID))
					}
				},
			)
			if err != nil {
				return
			}
		}
	}()

	unsub := func() {
		cancel()
	}
	return ch, unsub, nil
}
