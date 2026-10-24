// Package valkey menyediakan Valkey (Redis-compatible) client.
package valkey

import (
	"crypto/tls"
	"fmt"

	"github.com/refreshTechRusuh/booking-service/internal/config"
	vk "github.com/valkey-io/valkey-go"
)

// NewClient membuat valkey.Client dari konfigurasi.
func NewClient(cfg config.ValkeyConfig) (vk.Client, error) {
	opts := vk.ClientOption{
		InitAddress: []string{cfg.Addr},
	}
	if cfg.Password != "" {
		opts.Password = cfg.Password
	}
	if cfg.TLS {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	client, err := vk.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("valkey: new client: %w", err)
	}
	return client, nil
}
