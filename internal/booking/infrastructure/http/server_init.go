package http

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/refreshTechRusuh/booking-service/internal/booking/application/usecase"
	"github.com/refreshTechRusuh/booking-service/internal/booking/infrastructure/cache"
	bookingdb "github.com/refreshTechRusuh/booking-service/internal/booking/infrastructure/db"
	"github.com/refreshTechRusuh/booking-service/internal/booking/infrastructure/event"
	"github.com/refreshTechRusuh/booking-service/internal/config"
	"github.com/refreshTechRusuh/booking-service/pkg/kafka"
	vk "github.com/valkey-io/valkey-go"
)

// NewWiredHandler mewiring semua dependency untuk booking domain dan mengembalikan Handler.
// Dipanggil dari delivery/server.go.
func NewWiredHandler(
	_ context.Context,
	cfg *config.Config,
	pool *pgxpool.Pool,
	valkeyClient vk.Client,
	producer *kafka.Producer,
) *Handler {
	log := slog.Default()

	bookingRepo := bookingdb.NewBookingRepository(pool)
	stockRepo := bookingdb.NewStockRepository(pool)
	dukcapilCl := newDukcapilClient(cfg.Dukcapil, log)
	cacheRepo := cache.NewCacheRepository(valkeyClient, cfg.JWT.AccessTokenSecret, log)
	eventProducer := event.NewKafkaProducer(producer, log)

	uc := usecase.NewBookingUsecase(pool, bookingRepo, stockRepo, dukcapilCl, cacheRepo, eventProducer, log)
	return NewHandler(uc)
}
