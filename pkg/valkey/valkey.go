// Package valkey menyediakan Valkey (Redis-compatible) client.
package valkey

import (
	"crypto/tls"
	"fmt"

	"github.com/refreshTechRusuh/booking-service/internal/config"
	vk "github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeyotel"
)

// NewClient membuat valkey.Client dari konfigurasi.
// valkeyotel.NewClient() digunakan agar setiap command menghasilkan child span OTel.
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

	// valkeyotel.NewClient wraps ClientOption dan menambahkan OTel hook.
	client, err := valkeyotel.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("valkey: new client: %w", err)
	}
	return client, nil
}
