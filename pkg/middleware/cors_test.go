package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/refreshTechRusuh/booking-service/pkg/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func preflight(t *testing.T, h http.Handler, origin string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodOptions, "/api/x", nil)
	req.Header.Set("Origin", origin)
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCORS_ShouldAllowCustomTokenHeaders_WhenPreflight(t *testing.T) {
	h := middleware.CORSMiddleware("*")(okHandler)

	rec := preflight(t, h, "http://localhost:3000")

	require.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, "http://localhost:3000", rec.Header().Get("Access-Control-Allow-Origin"))
	allowed := strings.ToLower(rec.Header().Get("Access-Control-Allow-Headers"))
	for _, hdr := range []string{"x-access-token", "x-payment-token", "authorization", "content-type", "x-transaction-id", "cache-control", "last-event-id", "accept"} {
		assert.Contains(t, allowed, hdr)
	}
}

func TestCORS_ShouldNotSetHeaders_WhenOriginNotAllowed(t *testing.T) {
	h := middleware.CORSMiddleware("https://app.example.com")(okHandler)

	rec := preflight(t, h, "https://evil.example.com")

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))
}

func TestCORS_ShouldPassThrough_WhenNotPreflight(t *testing.T) {
	h := middleware.CORSMiddleware("*")(okHandler)
	req := httptest.NewRequest(http.MethodGet, "/api/x", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "Origin", rec.Header().Get("Vary"))
}
