// Package middleware menyediakan middleware HTTP untuk booking-service.
package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

type contextKey string

const (
	ctxUserID      contextKey = "userId"
	ctxAccessToken contextKey = "accessTokenClaims"
)

// AccessTokenClaims adalah claim dari Access Token yang diterbitkan Queue Service.
type AccessTokenClaims struct {
	EventID             string `json:"eventId"`
	QueueToken          string `json:"queueToken"`
	NIK                 string `json:"nik,omitempty"` // kosong untuk guest yang belum isi NIK
	BookingTTLExpiresAt int64  `json:"bookingTtlExpiresAt"` // unix timestamp
	jwt.RegisteredClaims
}

// OptionalAuth mencoba membaca Authorization: Bearer JWT.
// Jika ada dan sah → isi userId di context. Jika tidak ada → lanjut tanpa menolak (guest).
func OptionalAuth(secret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if token := extractBearer(r); token != "" {
				if userID, ok := parseUserJWT(token, secret); ok {
					r = r.WithContext(context.WithValue(r.Context(), ctxUserID, userID))
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

// RequireAccessToken validas X-Access-Token header (JWT dari Queue Service).
// Jika tidak ada atau tidak sah → 401.
func RequireAccessToken(secret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tokenStr := r.Header.Get("X-Access-Token")
			if tokenStr == "" {
				http.Error(w, `{"status":"401","message":"Access Token tidak ada","data":{}}`, http.StatusUnauthorized)
				return
			}
			claims, err := parseAccessToken(tokenStr, secret)
			if err != nil {
				http.Error(w, `{"status":"401","message":"Access Token tidak valid","data":{}}`, http.StatusUnauthorized)
				return
			}
			r = r.WithContext(context.WithValue(r.Context(), ctxAccessToken, claims))
			next.ServeHTTP(w, r)
		})
	}
}

// GetAccessTokenClaims mengambil AccessTokenClaims dari context (sudah divalidasi oleh RequireAccessToken).
func GetAccessTokenClaims(ctx context.Context) (*AccessTokenClaims, bool) {
	v, ok := ctx.Value(ctxAccessToken).(*AccessTokenClaims)
	return v, ok
}

// GetUserID mengambil userId dari context (di-set oleh OptionalAuth jika ada JWT).
func GetUserID(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(ctxUserID).(string)
	return v, ok
}

// TransactionID middleware menambahkan X-Transaction-Id ke response header.
func TransactionID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		txID := r.Header.Get("X-Transaction-Id")
		if txID == "" {
			txID = generateTxID()
		}
		w.Header().Set("X-Transaction-Id", txID)
		next.ServeHTTP(w, r)
	})
}

// Chain menyusun middleware dari luar ke dalam (pertama = paling luar).
func Chain(h http.Handler, middlewares ...func(http.Handler) http.Handler) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}

// ---- helpers ---------------------------------------------------------------

func extractBearer(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimPrefix(auth, "Bearer ")
	}
	return ""
}

func parseUserJWT(tokenStr, secret string) (string, bool) {
	token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, jwt.ErrSignatureInvalid
		}
		return []byte(secret), nil
	})
	if err != nil || !token.Valid {
		return "", false
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return "", false
	}
	sub, _ := claims["sub"].(string)
	return sub, sub != ""
}

func parseAccessToken(tokenStr, secret string) (*AccessTokenClaims, error) {
	claims := &AccessTokenClaims{}
	_, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, jwt.ErrSignatureInvalid
		}
		return []byte(secret), nil
	})
	if err != nil {
		return nil, err
	}
	return claims, nil
}

func generateTxID() string {
	// Implementasi sederhana — ganti dengan UUID atau snowflake di production.
	// import "github.com/google/uuid" → uuid.New().String()
	return "txid-placeholder"
}
