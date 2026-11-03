// Package token menyediakan implementasi JWT signer untuk Payment Token.
package token

import (
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// PaymentTokenClaims adalah claims JWT Payment Token.
type PaymentTokenClaims struct {
	BookingID string `json:"bookingId"`
	EventID   string `json:"eventId"`
	jwt.RegisteredClaims
}

// PaymentTokenSigner mengimplementasikan outbound.IPaymentTokenSigner.
type PaymentTokenSigner struct {
	secret []byte
}

// NewPaymentTokenSigner membuat signer baru dengan secret yang diberikan.
func NewPaymentTokenSigner(secret string) *PaymentTokenSigner {
	return &PaymentTokenSigner{secret: []byte(secret)}
}

// Sign menerbitkan JWT HS256 dengan bookingId, eventId, dan exp = paymentExpiresAt.
func (s *PaymentTokenSigner) Sign(bookingID, eventID string, paymentExpiresAt int64) (string, error) {
	now := time.Now()
	claims := PaymentTokenClaims{
		BookingID: bookingID,
		EventID:   eventID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "booking-service",
			Subject:   bookingID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(time.Unix(paymentExpiresAt, 0)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(s.secret)
}
