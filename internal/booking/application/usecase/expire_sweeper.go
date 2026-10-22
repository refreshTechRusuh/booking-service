package usecase

import (
	"context"
	"log/slog"
	"time"

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
// UPDATE status=EXPIRED + ReleaseStock + publish events.
func (u *BookingUsecase) expireBooking(ctx context.Context, bookingID, eventID string) error {
	// Gunakan CreateBookingTx-like pattern via pool langsung (sweeper punya pool)
	tx, err := u.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := u.bookingRepo.UpdateStatus(ctx, tx, bookingID, entity.BookingStatusExpired); err != nil {
		return err
	}
	if err := u.stockRepo.ReleaseStock(ctx, tx, bookingID); err != nil {
		return err
	}

	// Baca items untuk Kafka payload (masih dalam tx)
	_, items, _ := u.bookingRepo.GetByID(ctx, bookingID)

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// Publish events setelah commit
	go func() {
		ctx2 := context.WithoutCancel(ctx)
		u.producer.PublishSlotReleased(ctx2, eventID, kafka.SlotReleasedEvent{
			BookingID: bookingID, EventID: eventID, Reason: "BOOKING_TTL",
		})
		for _, item := range items {
			u.producer.PublishStockChanged(ctx2, eventID, kafka.StockChangedEvent{
				EventID: eventID, CategoryID: item.CategoryID, Delta: 1, Available: -1, Reason: "RELEASE",
			})
		}
		u.log.InfoContext(ctx2, "booking expired, stok dirilis",
			slog.String("bookingId", bookingID), slog.Int("items", len(items)))
	}()

	return nil
}
