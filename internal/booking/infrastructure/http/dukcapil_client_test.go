// Package http — white-box test untuk dukcapilClient (unexported).
package http

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/refreshTechRusuh/booking-service/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dukcapilDiscardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(nil, &slog.HandlerOptions{Level: slog.LevelError + 10}))
}

// mockDukcapilServer membuat httptest.Server yang menyimulasikan response Dukcapil.
func mockDukcapilServer(t *testing.T, valid bool, statusCode int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/dukcapil/v1/verify", r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
			"status":  "OK",
			"message": "success",
			"data":    map[string]bool{"valid": valid},
		})
	}))
}

// ── newDukcapilClient ─────────────────────────────────────────────────────────

func TestNewDukcapilClient_defaults(t *testing.T) {
	cfg := config.DukcapilConfig{
		BaseURL:   "http://localhost:8085",
		TimeoutMs: 3000,
		FailOpen:  true,
	}
	c := newDukcapilClient(cfg, dukcapilDiscardLog())
	assert.NotNil(t, c)
	assert.Equal(t, "http://localhost:8085", c.baseURL)
	assert.True(t, c.failOpen)
}

// ── Verify — success paths ────────────────────────────────────────────────────

func TestDukcapilClient_Verify_validNIK(t *testing.T) {
	srv := mockDukcapilServer(t, true, http.StatusOK)
	defer srv.Close()

	cfg := config.DukcapilConfig{BaseURL: srv.URL, TimeoutMs: 3000, FailOpen: false}
	c := newDukcapilClient(cfg, dukcapilDiscardLog())

	valid, err := c.Verify(context.Background(), "3273000000000001", "Budi Santoso", "1992-05-10", "Siti Rahayu")
	require.NoError(t, err)
	assert.True(t, valid)
}

func TestDukcapilClient_Verify_invalidNIK(t *testing.T) {
	srv := mockDukcapilServer(t, false, http.StatusOK)
	defer srv.Close()

	cfg := config.DukcapilConfig{BaseURL: srv.URL, TimeoutMs: 3000, FailOpen: false}
	c := newDukcapilClient(cfg, dukcapilDiscardLog())

	valid, err := c.Verify(context.Background(), "3273000000000099", "Salah Nama", "2000-01-01", "Beda Ibu")
	require.NoError(t, err)
	assert.False(t, valid)
}

func TestDukcapilClient_Verify_requestBody_correct(t *testing.T) {
	var received dukcapilVerifyRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received) //nolint:errcheck
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"data": map[string]bool{"valid": true}}) //nolint:errcheck
	}))
	defer srv.Close()

	cfg := config.DukcapilConfig{BaseURL: srv.URL, TimeoutMs: 3000, FailOpen: false}
	c := newDukcapilClient(cfg, dukcapilDiscardLog())

	c.Verify(context.Background(), "3273000000000001", "Budi Santoso", "1992-05-10", "Siti Rahayu") //nolint:errcheck

	assert.Equal(t, "3273000000000001", received.NIK)
	assert.Equal(t, "Budi Santoso", received.Nama)
	assert.Equal(t, "1992-05-10", received.TglLahir)
	assert.Equal(t, "Siti Rahayu", received.NamaIbu)
}

// ── Verify — fail-open paths ─────────────────────────────────────────────────

func TestDukcapilClient_Verify_failOpen_serverDown(t *testing.T) {
	// Server tidak ada — sambungan ditolak
	cfg := config.DukcapilConfig{BaseURL: "http://127.0.0.1:19998", TimeoutMs: 500, FailOpen: true}
	c := newDukcapilClient(cfg, dukcapilDiscardLog())

	// NIK 16 digit → fail-open return true
	valid, err := c.Verify(context.Background(), "3273000000000001", "A", "B", "C")
	require.NoError(t, err)
	assert.True(t, valid, "fail-open: NIK 16 digit diterima")
}

func TestDukcapilClient_Verify_failClose_serverDown(t *testing.T) {
	cfg := config.DukcapilConfig{BaseURL: "http://127.0.0.1:19998", TimeoutMs: 500, FailOpen: false}
	c := newDukcapilClient(cfg, dukcapilDiscardLog())

	_, err := c.Verify(context.Background(), "3273000000000001", "A", "B", "C")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dukcapil client")
}

func TestDukcapilClient_Verify_failOpen_badJSON(t *testing.T) {
	// Server mengembalikan response yang bukan JSON valid
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not-json-at-all")) //nolint:errcheck
	}))
	defer srv.Close()

	cfg := config.DukcapilConfig{BaseURL: srv.URL, TimeoutMs: 3000, FailOpen: true}
	c := newDukcapilClient(cfg, dukcapilDiscardLog())

	valid, err := c.Verify(context.Background(), "3273000000000001", "A", "B", "C")
	require.NoError(t, err)
	assert.True(t, valid, "fail-open: decode error tetap terima NIK 16 digit")
}

func TestDukcapilClient_Verify_failClose_badJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not-json")) //nolint:errcheck
	}))
	defer srv.Close()

	cfg := config.DukcapilConfig{BaseURL: srv.URL, TimeoutMs: 3000, FailOpen: false}
	c := newDukcapilClient(cfg, dukcapilDiscardLog())

	_, err := c.Verify(context.Background(), "3273000000000001", "A", "B", "C")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dukcapil decode")
}

func TestDukcapilClient_Verify_statusNonOK_stillReturnsResponse(t *testing.T) {
	// Status non-200 tapi body masih JSON valid — harus parse response
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{"data": map[string]bool{"valid": false}}) //nolint:errcheck
	}))
	defer srv.Close()

	cfg := config.DukcapilConfig{BaseURL: srv.URL, TimeoutMs: 3000, FailOpen: false}
	c := newDukcapilClient(cfg, dukcapilDiscardLog())

	valid, err := c.Verify(context.Background(), "3273000000000001", "A", "B", "C")
	require.NoError(t, err)
	assert.False(t, valid)
}
