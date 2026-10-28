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

// NewWiredHandlerWithUC mewiring semua dependency dan mengembalikan Handler + UseCase.
// UseCase diekspos agar cmd/main.go bisa start sweeper.
func NewWiredHandlerWithUC(
	_ context.Context,
	cfg *config.Config,
	pool *pgxpool.Pool,
	valkeyClient vk.Client,
	producer *kafka.Producer,
) (*Handler, *usecase.BookingUsecase) {
	log := slog.Default()

	bookingRepo := bookingdb.NewBookingRepository(pool, log)
	stockRepo := bookingdb.NewStockRepository(pool, log)
	dukcapilCl := newDukcapilClient(cfg.Dukcapil, log)
	cacheRepo := cache.NewCacheRepository(valkeyClient, cfg.JWT.AccessTokenSecret, log)
	eventProducer := event.NewKafkaProducer(producer, log)

	eventClient := NewEventServiceClient(cfg.EventSvc.BaseURL, log)

	uc := usecase.NewBookingUsecase(pool, bookingRepo, stockRepo, dukcapilCl, cacheRepo, eventProducer, eventClient, log)
	return NewHandler(uc), uc
}
