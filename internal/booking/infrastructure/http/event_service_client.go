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

// MarkSoldOut memanggil POST /internal/events/{eventId}/sold-out ke event-service.
// Idempoten: event-service mengembalikan 200 bila sudah sold-out sebelumnya.
func (c *EventServiceClient) MarkSoldOut(ctx context.Context, eventID string) error {
	url := fmt.Sprintf("%s/internal/events/%s/sold-out", c.baseURL, eventID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return fmt.Errorf("event service client: build request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("event service client: do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		return fmt.Errorf("event service client: server error %d", resp.StatusCode)
	}

	// 200, 409 (already sold out) sama-sama OK.
	c.log.InfoContext(ctx, "event sold-out marked",
		slog.String("eventId", eventID),
		slog.Int("status", resp.StatusCode))
	return nil
}
