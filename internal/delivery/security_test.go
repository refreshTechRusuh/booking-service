package delivery_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/refreshTechRusuh/booking-service/internal/config"
	"github.com/refreshTechRusuh/booking-service/internal/delivery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Menguji rangkaian middleware di delivery.New tanpa menyentuh database/Kafka:
// request ke route yang tak ada tetap melewati seluruh rantai middleware.

func newSecurityHandler(t *testing.T) http.Handler {
	t.Helper()
	cfg := &config.Config{}
	cfg.JWT.AccessTokenSecret = "test-access-secret"
	cfg.JWT.PaymentTokenSecret = "test-payment-secret"
	cfg.Dukcapil.BaseURL = "http://localhost:8085"
	cfg.Dukcapil.TimeoutMs = 100
	cfg.Dukcapil.FailOpen = true
	cfg.EventSvc.BaseURL = "http://localhost:8082"
	cfg.SSE.HeartbeatSeconds = 30
	app, err := delivery.New(context.Background(), cfg, nil, nil, nil)
	require.NoError(t, err)
	return app.Handler
}

func serveSecurity(h http.Handler, method, target string, body io.Reader, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, body)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestDelivery_ShouldSetSecurityHeaders_OnEveryKindOfResponse(t *testing.T) {
	h := newSecurityHandler(t)

	for name, rr := range map[string]*httptest.ResponseRecorder{
		"404 route tak ada": serveSecurity(h, http.MethodGet, "/tidak/ada", nil, nil),
		"preflight OPTIONS": serveSecurity(h, http.MethodOptions, "/tidak/ada", nil, map[string]string{"Origin": "https://fe.example"}),
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, "nosniff", rr.Header().Get("X-Content-Type-Options"))
			assert.Equal(t, "default-src 'none'", rr.Header().Get("Content-Security-Policy"))
			assert.Equal(t, "no-store", rr.Header().Get("Cache-Control"))
			assert.NotEmpty(t, rr.Header().Get("Strict-Transport-Security"))
			assert.Empty(t, rr.Header().Get("X-Frame-Options"))
		})
	}
}

func TestDelivery_ShouldReturn413_WithCORS_WhenBodyOverOneMB(t *testing.T) {
	h := newSecurityHandler(t)

	rr := serveSecurity(h, http.MethodPost, "/tidak/ada", strings.NewReader(strings.Repeat("a", (1<<20)+1)),
		map[string]string{"Origin": "https://fe.example"})

	assert.Equal(t, http.StatusRequestEntityTooLarge, rr.Code)
	assert.Equal(t, "https://fe.example", rr.Header().Get("Access-Control-Allow-Origin"),
		"413 harus membawa header CORS agar browser tidak melaporkan CORS error")
}
