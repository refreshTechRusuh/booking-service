package usecase

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/dto"
	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/entity"
	"github.com/refreshTechRusuh/booking-service/pkg/kafka"
)

const (
	sweeperInterval = 30 * time.Second
	sweeperBatch    = 50
)

// StartSweeper menjalankan sweeper TTL booking sebagai goroutine latar belakang.
// Berhenti ketika ctx di-cancel (graceful shutdown).
func (u *BookingUsecase) StartSweeper(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(sweeperInterval)
		defer ticker.Stop()
		u.log.InfoContext(ctx, "booking sweeper started", slog.Duration("interval", sweeperInterval))

		for {
			select {
			case <-ctx.Done():
				u.log.InfoContext(ctx, "booking sweeper stopped")
				return
			case <-ticker.C:
				u.sweepExpiredBookings(ctx)
			}
		}
	}()
}

// SweepOnce menjalankan satu siklus sweep synchronously (untuk testing).
func (u *BookingUsecase) SweepOnce(ctx context.Context) {
	u.sweepExpiredBookings(ctx)
}

// sweepExpiredBookings mendeteksi booking HELD yang TTL-nya habis dan memproses satu per satu.
func (u *BookingUsecase) sweepExpiredBookings(ctx context.Context) {
	bookings, err := u.bookingRepo.ListExpiredHeld(ctx, sweeperBatch)
	if err != nil {
		u.log.ErrorContext(ctx, "sweeper: list expired failed", slog.Any("err", err))
		return
	}
	if len(bookings) == 0 {
		return
	}

	u.log.InfoContext(ctx, "sweeper: processing expired bookings", slog.Int("count", len(bookings)))
	for _, b := range bookings {
		if err := u.expireBooking(ctx, b.ID, b.EventID); err != nil {
			u.log.ErrorContext(ctx, "sweeper: expire booking failed",
				slog.String("bookingId", b.ID), slog.Any("err", err))
		}
	}
}

// expireBooking mengeksekusi satu booking expired:
// UPDATE status=EXPIRED + ReleaseStock + publish Kafka + publish Redis Pub/Sub + sold-out detection.
func (u *BookingUsecase) expireBooking(ctx context.Context, bookingID, eventID string) error {
	// ExpireBookingTx: BEGIN + UPDATE booking + ReleaseStock + COMMIT, dikelola di repo.
	// Usecase tidak perlu pool.Begin langsung → fully testable tanpa DB.
	items, err := u.bookingRepo.ExpireBookingTx(ctx, bookingID)
	if err != nil {
		return err
	}

	// Publish events setelah commit (best-effort, tidak rollback expire)
	go u.postExpirePublish(context.WithoutCancel(ctx), bookingID, eventID, items)

	return nil
}

// postExpirePublish mempublish semua side-effect setelah booking di-expire.
// Berjalan sebagai goroutine terpisah agar tidak memblokir sweeper loop.
func (u *BookingUsecase) postExpirePublish(ctx context.Context, bookingID, eventID string, items []entity.BookingItem) {
	// 1. Kafka: slot.released → Queue Service promosikan antrian berikutnya.
	if err := u.producer.PublishSlotReleased(ctx, eventID, kafka.SlotReleasedEvent{
		BookingID: bookingID, EventID: eventID, Reason: "BOOKING_TTL",
	}); err != nil {
		u.log.ErrorContext(ctx, "publish slot.released failed", slog.Any("err", err))
	}

	// 2. Ambil snapshot stok terkini setelah release untuk Pub/Sub + Kafka.
	stocks, err := u.stockRepo.GetAvailableByEvent(ctx, eventID)
	if err != nil {
		u.log.WarnContext(ctx, "get available after release failed", slog.Any("err", err))
	}
	stockMap := make(map[string]int, len(stocks))
	for _, s := range stocks {
		stockMap[s.CategoryID] = s.Available
	}

	// 3. Per-kategori: publish Kafka stock.changed + Redis Pub/Sub (→ SSE ke klien).
	for _, item := range items {
		available := stockMap[item.CategoryID] // 0 bila tidak ditemukan

		// Kafka: stock.changed (reason=RELEASE) dengan nilai available aktual.
		if pubErr := u.producer.PublishStockChanged(ctx, eventID, kafka.StockChangedEvent{
			EventID:    eventID,
			CategoryID: item.CategoryID,
			Delta:      1,
			Available:  available,
			Reason:     "RELEASE",
		}); pubErr != nil {
			u.log.ErrorContext(ctx, "publish stock.changed failed", slog.Any("err", pubErr))
		}

		// Redis Pub/Sub: kirim frame stok terbaru langsung ke SSE subscriber.
		ev := dto.StockEvent{
			EventID:    eventID,
			CategoryID: item.CategoryID,
			Available:  available,
		}
		if b, marshalErr := json.Marshal(ev); marshalErr == nil {
			if pubErr := u.cache.PublishStockChanged(ctx, eventID, b); pubErr != nil {
				u.log.WarnContext(ctx, "redis publish stock.changed failed", slog.Any("err", pubErr))
			}
		}
	}

	// 4. Sold-out detection: hanya mark sold-out bila SEMUA available=0 DAN tidak ada HELD aktif.
	allZero := len(stocks) > 0
	for _, s := range stocks {
		if s.Available > 0 {
			allZero = false
			break
		}
	}
	if allZero {
		hasHeld, checkErr := u.bookingRepo.HasActiveHeld(ctx, eventID)
		if checkErr != nil {
			u.log.WarnContext(ctx, "has active held check failed", slog.Any("err", checkErr))
		}
		if !hasHeld {
			// Semua stok 0 dan tidak ada HELD aktif → event benar-benar habis.
			u.log.InfoContext(ctx, "event sold-out detected", slog.String("eventId", eventID))

			// 4a. Beritahu event-service untuk update status event (idempoten).
			if markErr := u.eventClient.MarkSoldOut(ctx, eventID); markErr != nil {
				u.log.WarnContext(ctx, "mark sold-out failed", slog.Any("err", markErr))
			}

			// 4b. Broadcast sold-out frame ke SSE subscriber via Redis Pub/Sub.
			soldOutEv := dto.StockEvent{EventID: eventID, SoldOut: true}
			if b, marshalErr := json.Marshal(soldOutEv); marshalErr == nil {
				if pubErr := u.cache.PublishStockChanged(ctx, eventID, b); pubErr != nil {
					u.log.WarnContext(ctx, "redis publish sold-out failed", slog.Any("err", pubErr))
				}
			}

			// 4c. Kafka: event.sold_out → downstream consumers.
			if pubErr := u.producer.PublishEventSoldOut(ctx, eventID, kafka.EventSoldOutEvent{
				EventID: eventID,
			}); pubErr != nil {
				u.log.ErrorContext(ctx, "publish event.sold_out failed", slog.Any("err", pubErr))
			}
		}
	}

	u.log.InfoContext(ctx, "booking expired processed",
		slog.String("bookingId", bookingID),
		slog.String("eventId", eventID),
		slog.Int("items", len(items)))
}
