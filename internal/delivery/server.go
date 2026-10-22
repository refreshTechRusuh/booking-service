// Package delivery adalah composition root booking-service.
// Semua wiring (repo → usecase → handler) dilakukan di sini.
package delivery

import (
	"context"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/refreshTechRusuh/booking-service/internal/config"
	bookinghttp "github.com/refreshTechRusuh/booking-service/internal/booking/infrastructure/http"
	"github.com/refreshTechRusuh/booking-service/pkg/kafka"
	"github.com/valkey-io/valkey-go"
)

// New mewiring semua dependency dan mengembalikan http.ServeMux + cleanup func.
func New(
	ctx context.Context,
	cfg *config.Config,
	pool *pgxpool.Pool,
	valkeyClient valkey.Client,
	producer *kafka.Producer,
) (mux *http.ServeMux, closeFn func(), err error) {
	mux = http.NewServeMux()

	// ---- Booking domain ----
	bookingHandler := bookinghttp.NewWiredHandler(ctx, cfg, pool, valkeyClient, producer)
	bookinghttp.RegisterRoutes(mux, bookingHandler, cfg.JWT.AccessTokenSecret)

	// Health check
	mux.HandleFunc("GET /sys/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"00000","message":"pong","data":{}}`))
	})

	closeFn = func() {
		producer.Close()
		pool.Close()
		valkeyClient.Close()
	}

	return mux, closeFn, nil
}
