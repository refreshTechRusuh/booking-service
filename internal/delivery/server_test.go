package delivery_test

import (
	"context"
	"testing"

	"github.com/refreshTechRusuh/booking-service/internal/config"
	"github.com/refreshTechRusuh/booking-service/internal/delivery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDelivery_New_nilDeps(t *testing.T) {
	// delivery.New menerima nil pool/valkeyClient/producer.
	// Dependency hanya disimpan (tidak digunakan) sampai handler dipanggil.
	// closeFn TIDAK dipanggil karena nil.Close() akan panic.
	ctx := context.Background()
	cfg := &config.Config{}
	cfg.JWT.AccessTokenSecret = "test-access-secret"
	cfg.JWT.PaymentTokenSecret = "test-payment-secret"
	cfg.Dukcapil.BaseURL = "http://localhost:8085"
	cfg.Dukcapil.TimeoutMs = 100
	cfg.Dukcapil.FailOpen = true
	cfg.EventSvc.BaseURL = "http://localhost:8082"
	cfg.SSE.HeartbeatSeconds = 30

	app, err := delivery.New(ctx, cfg, nil, nil, nil)
	require.NoError(t, err)
	assert.NotNil(t, app)
	assert.NotNil(t, app.Handler)
	assert.NotNil(t, app.Usecase)
	assert.NotNil(t, app.Close)
	// Tidak memanggil app.Close() karena pool/valkeyClient/producer = nil
}
