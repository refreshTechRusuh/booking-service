// Package http menyediakan HTTP adapter untuk panggilan ke service lain (outbound HTTP client).
package http

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// EventServiceClient memanggil event-service via HTTP internal API.
// Mengimplementasikan outbound.IEventServiceClient.
type EventServiceClient struct {
	baseURL string
	client  *http.Client
	log     *slog.Logger
}

// NewEventServiceClient membuat instance baru EventServiceClient.
func NewEventServiceClient(baseURL string, log *slog.Logger) *EventServiceClient {
	return &EventServiceClient{
		baseURL: baseURL,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
		log: log,
	}
}

// MarkSoldOut memanggil POST /internal/event/v1/{eventId}/sold-out ke event-service.
// Idempoten: event-service mengembalikan 200 bila sudah sold-out sebelumnya.
func (c *EventServiceClient) MarkSoldOut(ctx context.Context, eventID string) error {
	url := fmt.Sprintf("%s/internal/event/v1/%s/sold-out", c.baseURL, eventID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return fmt.Errorf("event service client: build request: %w", err)
	}

	start := time.Now()
	c.log.InfoContext(ctx, "event-service call: mark sold-out",
		slog.String("eventId", eventID),
		slog.String("url", url),
	)

	resp, err := c.client.Do(req)
	if err != nil {
		c.log.ErrorContext(ctx, "event-service call failed",
			slog.String("eventId", eventID),
			slog.String("duration", time.Since(start).String()),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("event service client: do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		c.log.ErrorContext(ctx, "event-service returned server error",
			slog.String("eventId", eventID),
			slog.Int("status", resp.StatusCode),
			slog.String("duration", time.Since(start).String()),
		)
		return fmt.Errorf("event service client: server error %d", resp.StatusCode)
	}

	// 200, 409 (already sold out) sama-sama OK.
	c.log.InfoContext(ctx, "event sold-out marked",
		slog.String("eventId", eventID),
		slog.Int("status", resp.StatusCode),
		slog.String("duration", time.Since(start).String()),
	)
	return nil
}
