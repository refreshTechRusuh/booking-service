package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/refreshTechRusuh/booking-service/pkg/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func userBearer(t *testing.T, secret, sub string) string {
	t.Helper()
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": sub, "role": "customer", "exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString([]byte(secret))
	require.NoError(t, err)
	return "Bearer " + tok
}

func runOptionalAuth(t *testing.T, secret, authHeader string) (string, bool) {
	t.Helper()
	var gotID string
	var gotOK bool
	h := middleware.OptionalAuth(secret)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotID, gotOK = middleware.GetUserID(r.Context())
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	return gotID, gotOK
}

func TestOptionalAuth_ShouldSetUserID_WhenSignedWithUserJWTSecret(t *testing.T) {
	id, ok := runOptionalAuth(t, "user-jwt-secret", userBearer(t, "user-jwt-secret", "user-123"))
	assert.True(t, ok)
	assert.Equal(t, "user-123", id)
}

func TestOptionalAuth_ShouldTreatAsGuest_WhenSignedWithOtherSecret(t *testing.T) {
	_, ok := runOptionalAuth(t, "user-jwt-secret", userBearer(t, testAccessSecret, "user-123"))
	assert.False(t, ok)
}

func TestOptionalAuth_ShouldTreatAsGuest_WhenNoAuthorizationHeader(t *testing.T) {
	_, ok := runOptionalAuth(t, "user-jwt-secret", "")
	assert.False(t, ok)
}
