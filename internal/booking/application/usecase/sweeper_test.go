package usecase_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/refreshTechRusuh/booking-service/internal/booking/application/usecase"
	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/entity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- sweepExpiredBookings branches ----------------------------------------

func TestSweepOnce_Empty_NoOp(t *testing.T) {
	// Tidak ada booking expired → sweeper diam saja
	bookingRepo := newMockBookingRepo()
	stockRepo := newMockStockRepo(map[string]int{}, map[string]int64{})
	uc := newUC(bookingRepo, stockRepo, newMockProducer())

	assert.NotPanics(t, func() {
		uc.SweepOnce(context.Background())
	})
}

func TestSweepOnce_ListExpiredError_Logs(t *testing.T) {
	// mockBookingRepo yang ListExpiredHeld selalu error
	repo := &errBookingRepo{err: errors.New("db error")}
	stockRepo := newMockStockRepo(map[string]int{}, map[string]int64{})
	uc := usecase.NewBookingUsecase(nil, repo, stockRepo,
		&mockDukcapil{}, &mockCache{}, newMockProducer(),
		&mockEventClient{}, &mockPaymentTokenSigner{}, slog.Default())

	assert.NotPanics(t, func() {
		uc.SweepOnce(context.Background())
	})
}

func TestSweepOnce_ExpireBookingError_Continues(t *testing.T) {
	// Booking expired ada, tapi ExpireBookingTx error → sweeper lanjut tanpa panic
	bookingRepo := newMockBookingRepo()
	pastTime := time.Now().Add(-5 * time.Minute)
	bookingRepo.bookings["bkg-err"] = entity.Booking{
		ID:               "bkg-err",
		EventID:          "evt-1",
		Status:           entity.BookingStatusHeld,
		PaymentExpiresAt: &pastTime,
	}
	// Override ExpireBookingTx to return error
	failRepo := &failExpireRepo{mockBookingRepo: bookingRepo}
	stockRepo := newMockStockRepo(map[string]int{"cat-1": 10}, map[string]int64{})
	uc := usecase.NewBookingUsecase(nil, failRepo, stockRepo,
		&mockDukcapil{}, &mockCache{}, newMockProducer(),
		&mockEventClient{}, &mockPaymentTokenSigner{}, slog.Default())

	assert.NotPanics(t, func() {
		uc.SweepOnce(context.Background())
	})
}

// ---- GetBooking branches ---------------------------------------------------

func TestGetBooking_PaymentExpiresAtNil_RemainingZero(t *testing.T) {
	bookingRepo := newMockBookingRepo()
	// Booking tanpa PaymentExpiresAt → remainingSeconds = 0, backDisabled = true
	bookingRepo.bookings["bkg-nil"] = entity.Booking{
		ID:               "bkg-nil",
		EventID:          "evt-1",
		Status:           entity.BookingStatusHeld,
		PaymentExpiresAt: nil,
	}
	uc := newUC(bookingRepo, newMockStockRepo(map[string]int{}, map[string]int64{}), newMockProducer())

	resp, err := uc.GetBooking(context.Background(), "bkg-nil")
	require.NoError(t, err)
	assert.Equal(t, "bkg-nil", resp.BookingID)
	assert.Equal(t, 0, resp.RemainingSeconds)
	assert.True(t, resp.BackDisabled) // < backDisabledThreshold
}

func TestGetBooking_PaymentExpired_RemainingZero(t *testing.T) {
	bookingRepo := newMockBookingRepo()
	past := time.Now().Add(-1 * time.Minute)
	bookingRepo.bookings["bkg-past"] = entity.Booking{
		ID:               "bkg-past",
		Status:           entity.BookingStatusHeld,
		PaymentExpiresAt: &past,
	}
	uc := newUC(bookingRepo, newMockStockRepo(map[string]int{}, map[string]int64{}), newMockProducer())

	resp, err := uc.GetBooking(context.Background(), "bkg-past")
	require.NoError(t, err)
	assert.Equal(t, 0, resp.RemainingSeconds)
}

func TestGetBooking_NotFound(t *testing.T) {
	uc := newUC(newMockBookingRepo(), newMockStockRepo(map[string]int{}, map[string]int64{}), newMockProducer())
	_, err := uc.GetBooking(context.Background(), "nonexistent")
	assert.Error(t, err)
}

// ---- postExpirePublish branches (via SweepOnce) ----------------------------

func TestSweepOnce_PublishesKafkaOnExpiry(t *testing.T) {
	bookingRepo := newMockBookingRepo()
	prod := newMockProducer()
	past := time.Now().Add(-5 * time.Minute)
	bookingRepo.bookings["bkg-exp"] = entity.Booking{
		ID: "bkg-exp", EventID: "evt-1",
		Status:           entity.BookingStatusHeld,
		PaymentExpiresAt: &past,
	}
	stockRepo := newMockStockRepo(map[string]int{"cat-1": 5}, map[string]int64{})

	uc := newUC(bookingRepo, stockRepo, prod)
	uc.SweepOnce(context.Background())

	// Tunggu goroutine postExpirePublish selesai
	time.Sleep(50 * time.Millisecond)
	assert.GreaterOrEqual(t, prod.count("slot.released"), 1, "slot.released harus dipublish")
}

// ---- StreamStock branches --------------------------------------------------

func TestStreamStock_SnapshotError(t *testing.T) {
	// stockRepo.GetAvailableByEvent error → StreamStock harus return error
	bookingRepo := newMockBookingRepo()
	// Pakai mockStockRepo yang dioverride GetAvailableByEvent via errStockRepo
	stockRepo := newMockStockRepo(map[string]int{}, map[string]int64{})
	errRepo := &errOnGetAvailable{mockStockRepo: stockRepo}
	uc := usecase.NewBookingUsecase(nil, bookingRepo, errRepo,
		&mockDukcapil{}, &mockCache{}, newMockProducer(),
		&mockEventClient{}, &mockPaymentTokenSigner{}, slog.Default())

	_, err := uc.StreamStock(context.Background(), "evt-1")
	assert.Error(t, err)
}

// ---- Mock helpers ----------------------------------------------------------

// errBookingRepo wraps mockBookingRepo but returns error on ListExpiredHeld
type errBookingRepo struct {
	*mockBookingRepo
	err error
}

func (r *errBookingRepo) ListExpiredHeld(_ context.Context, _ int) ([]entity.Booking, error) {
	return nil, r.err
}

// failExpireRepo wraps mockBookingRepo but returns error on ExpireBookingTx
type failExpireRepo struct {
	*mockBookingRepo
}

func (r *failExpireRepo) ExpireBookingTx(_ context.Context, _ string) ([]entity.BookingItem, error) {
	return nil, errors.New("expire failed")
}

// errOnGetAvailable wraps mockStockRepo but returns error on GetAvailableByEvent
type errOnGetAvailable struct {
	*mockStockRepo
}

func (r *errOnGetAvailable) GetAvailableByEvent(_ context.Context, _ string) ([]entity.CategoryStock, error) {
	return nil, errors.New("db error")
}
