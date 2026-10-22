// Package delivery adalah composition root booking-service.
package delivery

import (
	"context"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/refreshTechRusuh/booking-service/internal/config"
	bookinghttp "github.com/refreshTechRusuh/booking-service/internal/booking/infrastructure/http"
	"github.com/refreshTechRusuh/booking-service/internal/booking/application/usecase"
	"github.com/refreshTechRusuh/booking-service/pkg/kafka"
	vk "github.com/valkey-io/valkey-go"
)

// Result dari New — ekspos BookingUsecase agar cmd/main.go bisa start sweeper.
type App struct {
	Mux     *http.ServeMux
	Usecase *usecase.BookingUsecase
	Close   func()
}

// New mewiring semua dependency dan mengembalikan App.
func New(
	ctx context.Context,
	cfg *config.Config,
	pool *pgxpool.Pool,
	valkeyClient vk.Client,
	producer *kafka.Producer,
) (*App, error) {
	mux := http.NewServeMux()

	handler, uc := bookinghttp.NewWiredHandlerWithUC(ctx, cfg, pool, valkeyClient, producer)
	bookinghttp.RegisterRoutes(mux, handler, cfg.JWT.AccessTokenSecret)

	mux.HandleFunc("GET /sys/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"00000","message":"pong","data":{}}`))
	})

	closeFn := func() {
		producer.Close()
		pool.Close()
		valkeyClient.Close()
	}

	return &App{Mux: mux, Usecase: uc, Close: closeFn}, nil
}
