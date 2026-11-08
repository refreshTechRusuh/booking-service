package token_test

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/refreshTechRusuh/booking-service/internal/booking/infrastructure/token"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testSecret = "payment-test-secret"

func TestPaymentTokenSigner_Sign_valid(t *testing.T) {
	signer := token.NewPaymentTokenSigner(testSecret)

	expiresAt := time.Now().Add(10 * time.Minute).Unix()
	tok, err := signer.Sign("booking-1", "event-1", expiresAt)
	require.NoError(t, err)
	assert.NotEmpty(t, tok)
}

func TestPaymentTokenSigner_Sign_claimsCorrect(t *testing.T) {
	signer := token.NewPaymentTokenSigner(testSecret)

	expiresAt := time.Now().Add(15 * time.Minute).Unix()
	tokStr, err := signer.Sign("booking-abc", "event-xyz", expiresAt)
	require.NoError(t, err)

	// Parse ulang dan verifikasi claims.
	type paymentClaims struct {
		BookingID string `json:"bookingId"`
		EventID   string `json:"eventId"`
		jwt.RegisteredClaims
	}
	claims := &paymentClaims{}
	_, err = jwt.ParseWithClaims(tokStr, claims, func(t *jwt.Token) (any, error) {
		return []byte(testSecret), nil
	})
	require.NoError(t, err)
	assert.Equal(t, "booking-abc", claims.BookingID)
	assert.Equal(t, "event-xyz", claims.EventID)
	assert.Equal(t, "booking-service", claims.Issuer)
	assert.Equal(t, "booking-abc", claims.Subject)
}

func TestPaymentTokenSigner_Sign_expiryRespected(t *testing.T) {
	signer := token.NewPaymentTokenSigner(testSecret)

	// Token kadaluwarsa 1 menit yang lalu
	expiresAt := time.Now().Add(-1 * time.Minute).Unix()
	tokStr, err := signer.Sign("booking-1", "event-1", expiresAt)
	require.NoError(t, err)

	type paymentClaims struct {
		BookingID string `json:"bookingId"`
		jwt.RegisteredClaims
	}
	claims := &paymentClaims{}
	_, err = jwt.ParseWithClaims(tokStr, claims, func(t *jwt.Token) (any, error) {
		return []byte(testSecret), nil
	})
	// Harus error karena token expired
	require.Error(t, err)
	assert.ErrorIs(t, err, jwt.ErrTokenExpired)
}

func TestPaymentTokenSigner_Sign_wrongSecret_rejectsVerification(t *testing.T) {
	signer := token.NewPaymentTokenSigner(testSecret)

	expiresAt := time.Now().Add(10 * time.Minute).Unix()
	tokStr, err := signer.Sign("booking-1", "event-1", expiresAt)
	require.NoError(t, err)

	type paymentClaims struct{ jwt.RegisteredClaims }
	claims := &paymentClaims{}
	_, err = jwt.ParseWithClaims(tokStr, claims, func(t *jwt.Token) (any, error) {
		return []byte("wrong-secret"), nil
	})
	require.Error(t, err)
}

func TestPaymentTokenSigner_Sign_differentBookings_differentTokens(t *testing.T) {
	signer := token.NewPaymentTokenSigner(testSecret)
	exp := time.Now().Add(10 * time.Minute).Unix()

	tok1, err := signer.Sign("booking-1", "event-1", exp)
	require.NoError(t, err)
	tok2, err := signer.Sign("booking-2", "event-1", exp)
	require.NoError(t, err)

	// Token harus berbeda walaupun event sama
	assert.NotEqual(t, tok1, tok2)
}
