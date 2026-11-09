// Package delivery adalah composition root booking-service.
package delivery

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/refreshTechRusuh/booking-service/internal/booking/application/usecase"
	bookinghttp "github.com/refreshTechRusuh/booking-service/internal/booking/infrastructure/http"
	"github.com/refreshTechRusuh/booking-service/internal/config"
	"github.com/refreshTechRusuh/booking-service/pkg/kafka"
	"github.com/refreshTechRusuh/booking-service/pkg/middleware"
	vk "github.com/valkey-io/valkey-go"
)

// Result dari New — ekspos BookingUsecase agar cmd/main.go bisa start sweeper.
type App struct {
	Handler http.Handler // mux yang sudah di-wrap logging middleware
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
	bookinghttp.RegisterRoutes(mux, handler, cfg.JWT.AccessTokenSecret, cfg.JWT.PaymentTokenSecret)

	mux.HandleFunc("GET /sys/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"00000","message":"pong","data":{}}`)) // #nosec G104 -- health endpoint, write errors ignorable
	})

	// Wrap seluruh mux dengan request logger — catat semua inbound request.
	logged := middleware.RequestLogger(slog.Default())(mux)
	traced := middleware.OtelMiddleware("booking-service")(logged)
	corsed := middleware.CORSMiddleware("*")(traced)

	closeFn := func() {
		producer.Close() // #nosec G104 -- shutdown, error irrelevant
		pool.Close()
		valkeyClient.Close()
	}

	return &App{Handler: corsed, Usecase: uc, Close: closeFn}, nil
}
