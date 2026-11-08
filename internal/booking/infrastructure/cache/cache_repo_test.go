package cache_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/refreshTechRusuh/booking-service/internal/booking/infrastructure/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	vk "github.com/valkey-io/valkey-go"
)

const testSecret = "test-secret-key-for-unit-tests"

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(nil, &slog.HandlerOptions{Level: slog.LevelError + 10}))
}

// newDummyClient membuat valkey Client yang menunjuk ke alamat fiktif.
// ValidateAccessToken TIDAK memanggil redis — murni JWT parsing.
// Kita pass nil karena tidak ada operasi Redis yang dipanggil.
func newNilClient() vk.Client {
	return nil
}

// makeToken membuat JWT HS256 dengan claims yang diberikan.
func makeToken(t *testing.T, secret, eventID, queueToken, nik string, ttlExpMs int64, expiry time.Duration) string {
	t.Helper()
	type accessClaims struct {
		EventID             string `json:"eventId"`
		QueueToken          string `json:"queueToken"`
		NIK                 string `json:"nik,omitempty"`
		BookingTTLExpiresAt int64  `json:"bookingTtlExpiresAt"`
		jwt.RegisteredClaims
	}
	c := &accessClaims{
		EventID:             eventID,
		QueueToken:          queueToken,
		NIK:                 nik,
		BookingTTLExpiresAt: ttlExpMs,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(expiry)),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, c)
	s, err := tok.SignedString([]byte(secret))
	require.NoError(t, err)
	return s
}

// ── ValidateAccessToken ───────────────────────────────────────────────────────

func TestCacheRepository_ValidateAccessToken_valid(t *testing.T) {
	repo := cache.NewCacheRepository(newNilClient(), testSecret, discardLog())

	ttlMs := time.Now().Add(10 * time.Minute).UnixMilli()
	tok := makeToken(t, testSecret, "event-1", "queue-token", "3273000000000001", ttlMs, 10*time.Minute)

	claims, err := repo.ValidateAccessToken(context.Background(), tok)
	require.NoError(t, err)
	assert.Equal(t, "event-1", claims.EventID)
	assert.Equal(t, "queue-token", claims.QueueToken)
	assert.Equal(t, "3273000000000001", claims.NIK)
	assert.Equal(t, ttlMs, claims.BookingTTLExpiresAt)
}

func TestCacheRepository_ValidateAccessToken_wrongSecret(t *testing.T) {
	repo := cache.NewCacheRepository(newNilClient(), testSecret, discardLog())

	ttlMs := time.Now().Add(10 * time.Minute).UnixMilli()
	tok := makeToken(t, "wrong-secret", "event-1", "queue-token", "", ttlMs, 10*time.Minute)

	_, err := repo.ValidateAccessToken(context.Background(), tok)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "validate access token")
}

func TestCacheRepository_ValidateAccessToken_expiredJWT(t *testing.T) {
	repo := cache.NewCacheRepository(newNilClient(), testSecret, discardLog())

	ttlMs := time.Now().Add(-5 * time.Minute).UnixMilli()
	tok := makeToken(t, testSecret, "event-1", "queue-token", "", ttlMs, -5*time.Minute)

	_, err := repo.ValidateAccessToken(context.Background(), tok)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "validate access token")
}

func TestCacheRepository_ValidateAccessToken_malformed(t *testing.T) {
	repo := cache.NewCacheRepository(newNilClient(), testSecret, discardLog())

	_, err := repo.ValidateAccessToken(context.Background(), "ini.bukan.jwt")
	require.Error(t, err)
}

func TestCacheRepository_ValidateAccessToken_emptyToken(t *testing.T) {
	repo := cache.NewCacheRepository(newNilClient(), testSecret, discardLog())

	_, err := repo.ValidateAccessToken(context.Background(), "")
	require.Error(t, err)
}

func TestCacheRepository_ValidateAccessToken_withoutNIK(t *testing.T) {
	repo := cache.NewCacheRepository(newNilClient(), testSecret, discardLog())

	ttlMs := time.Now().Add(10 * time.Minute).UnixMilli()
	// NIK kosong = token dari user yang tidak perlu NIK (opsional)
	tok := makeToken(t, testSecret, "event-2", "qt-2", "", ttlMs, 10*time.Minute)

	claims, err := repo.ValidateAccessToken(context.Background(), tok)
	require.NoError(t, err)
	assert.Equal(t, "event-2", claims.EventID)
	assert.Equal(t, "", claims.NIK)
}
