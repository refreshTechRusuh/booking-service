// Package cache mengimplementasikan outbound.ICacheRepository via Valkey.
package cache

import (
	"context"
	"fmt"
	"log/slog"
	"time"

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
//
// TODO(BE-1-INTEGRATION): Saat BE-1 selesai:
//  1. Pastikan Queue Service mengeluarkan JWT HS256 dengan secret yang sama
//  2. Jika Queue Service menambahkan revocation via Redis (blacklist token), tambahkan
//     validasi Redis di sini: r.client.Do(ctx, GET token:revoked:{jti})
//  3. Jika Queue Service memakai RS256 (asimetris), ganti HS256 ke ParseRSA dan
//     inject public key sebagai config baru (JWT_PUBLIC_KEY_PATH)
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
	start := time.Now()
	err := r.client.Do(ctx,
		r.client.B().Publish().Channel(channel).Message(string(payload)).Build(),
	).Error()
	elapsed := fmt.Sprintf("%s", time.Since(start))
	if err != nil {
		r.log.ErrorContext(ctx, "redis publish failed",
			slog.String("channel", channel),
			slog.String("duration", elapsed),
			slog.String("error", err.Error()),
		)
	} else {
		r.log.InfoContext(ctx, "redis publish ok",
			slog.String("channel", channel),
			slog.Int("bytes", len(payload)),
			slog.String("duration", elapsed),
		)
	}
	return err
}

// SubscribeStockChanged subscribe ke channel Redis Pub/Sub untuk SSE stok.
func (r *CacheRepository) SubscribeStockChanged(ctx context.Context, eventID string) (<-chan []byte, func(), error) {
	channel := fmt.Sprintf("stock:%s", eventID)
	ch := make(chan []byte, 32)

	// valkey-go: Dedicated Pub/Sub client.
	dedicatedClient, cancel := r.client.Dedicate()

	go func() {
		defer close(ch)

		// valkey-go idiom: Receive() MENGIRIM perintah SUBSCRIBE lalu memblok
		// selama koneksi hidup, memanggil callback untuk setiap pesan. Jangan
		// kirim SUBSCRIBE terpisah lewat Do() sebelumnya — itu membuat koneksi
		// sudah masuk mode subscribe sehingga Receive gagal dan subscriber hilang.
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
		// Receive hanya kembali saat ctx dibatalkan (klien disconnect) atau koneksi putus.
		if err != nil && ctx.Err() == nil {
			r.log.ErrorContext(ctx, "subscribe ended with error", slog.String("channel", channel), slog.Any("err", err))
		}
	}()

	unsub := func() {
		cancel()
	}
	return ch, unsub, nil
}
