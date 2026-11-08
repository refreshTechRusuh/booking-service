package http_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	bookinghttp "github.com/refreshTechRusuh/booking-service/internal/booking/infrastructure/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func eventClientDiscardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(nil, &slog.HandlerOptions{Level: slog.LevelError + 10}))
}

// ── NewEventServiceClient ─────────────────────────────────────────────────────

func TestNewEventServiceClient_created(t *testing.T) {
	c := bookinghttp.NewEventServiceClient("http://localhost:8082", eventClientDiscardLog())
	assert.NotNil(t, c)
}

// ── MarkSoldOut ───────────────────────────────────────────────────────────────

func TestEventServiceClient_MarkSoldOut_success200(t *testing.T) {
	var capturedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		assert.Equal(t, http.MethodPost, r.Method)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := bookinghttp.NewEventServiceClient(srv.URL, eventClientDiscardLog())
	err := c.MarkSoldOut(context.Background(), "event-123")
	require.NoError(t, err)
	assert.Equal(t, "/internal/event/v1/event-123/sold-out", capturedPath)
}

func TestEventServiceClient_MarkSoldOut_success409(t *testing.T) {
	// 409 = sudah sold out sebelumnya, idempoten — harus return nil error
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer srv.Close()

	c := bookinghttp.NewEventServiceClient(srv.URL, eventClientDiscardLog())
	err := c.MarkSoldOut(context.Background(), "event-sold")
	require.NoError(t, err)
}

func TestEventServiceClient_MarkSoldOut_success204(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := bookinghttp.NewEventServiceClient(srv.URL, eventClientDiscardLog())
	err := c.MarkSoldOut(context.Background(), "event-sold")
	require.NoError(t, err)
}

func TestEventServiceClient_MarkSoldOut_serverError500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := bookinghttp.NewEventServiceClient(srv.URL, eventClientDiscardLog())
	err := c.MarkSoldOut(context.Background(), "event-err")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "server error 500")
}

func TestEventServiceClient_MarkSoldOut_serverError503(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c := bookinghttp.NewEventServiceClient(srv.URL, eventClientDiscardLog())
	err := c.MarkSoldOut(context.Background(), "event-err")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "server error 503")
}

func TestEventServiceClient_MarkSoldOut_connectionError(t *testing.T) {
	// Port tidak ada → connection refused
	c := bookinghttp.NewEventServiceClient("http://127.0.0.1:19997", eventClientDiscardLog())
	err := c.MarkSoldOut(context.Background(), "event-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "event service client: do request")
}

func TestEventServiceClient_MarkSoldOut_contextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel sebelum request

	c := bookinghttp.NewEventServiceClient(srv.URL, eventClientDiscardLog())
	err := c.MarkSoldOut(ctx, "event-cancel")
	// Context cancelled → request gagal
	require.Error(t, err)
}

func TestEventServiceClient_MarkSoldOut_multipleEventIDs(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := bookinghttp.NewEventServiceClient(srv.URL, eventClientDiscardLog())

	events := []string{"event-1", "event-2", "event-abc"}
	for _, id := range events {
		require.NoError(t, c.MarkSoldOut(context.Background(), id))
	}

	assert.Equal(t, "/internal/event/v1/event-1/sold-out", paths[0])
	assert.Equal(t, "/internal/event/v1/event-2/sold-out", paths[1])
	assert.Equal(t, "/internal/event/v1/event-abc/sold-out", paths[2])
}
