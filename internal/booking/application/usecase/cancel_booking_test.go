package usecase_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/refreshTechRusuh/booking-service/internal/booking/application/usecase"
	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/entity"
	"github.com/refreshTechRusuh/booking-service/internal/booking/port/outbound"
	"github.com/refreshTechRusuh/booking-service/pkg/apperror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newCancelUC(repo *mockBookingRepo, cache *mockCache, prod *mockProducer) *usecase.BookingUsecase {
	stock := newMockStockRepo(map[string]int{"cat-1": 5}, map[string]int64{})
	return usecase.NewBookingUsecase(nil, repo, stock, &mockDukcapil{}, cache, prod,
		&mockEventClient{}, &mockPaymentTokenSigner{}, slog.Default())
}

func heldBooking(repo *mockBookingRepo, id string) {
	repo.bookings[id] = entity.Booking{ID: id, EventID: "evt-1", Status: entity.BookingStatusHeld}
	repo.items[id] = []entity.BookingItem{{ID: "it-1", BookingID: id, CategoryID: "cat-1", NIK: "1234567890123456"}}
}

func TestCancelBooking_ShouldCancelAndPublish_WhenHeld(t *testing.T) {
	repo := newMockBookingRepo()
	heldBooking(repo, "bkg-1")
	cache := &mockCache{}
	require.NoError(t, cache.StoreBookingQueueToken(context.Background(), "bkg-1", "q_token", time.Minute))
	prod := newMockProducer()
	uc := newCancelUC(repo, cache, prod)

	resp, err := uc.CancelBooking(context.Background(), "bkg-1")

	require.NoError(t, err)
	assert.Equal(t, "CANCELLED", resp.Status)
	assert.Equal(t, entity.BookingStatusCancelled, repo.bookings["bkg-1"].Status)
	revoked, _ := cache.IsQueueTokenRevoked(context.Background(), "q_token")
	assert.True(t, revoked, "queue token harus dicabut")
	assert.Eventually(t, func() bool {
		return prod.count("slot.released") == 1 && prod.count("stock.changed") == 1
	}, time.Second, 10*time.Millisecond)
}

func TestCancelBooking_ShouldBeIdempotent_WhenAlreadyCancelled(t *testing.T) {
	repo := newMockBookingRepo()
	heldBooking(repo, "bkg-1")
	prod := newMockProducer()
	uc := newCancelUC(repo, &mockCache{}, prod)

	_, err := uc.CancelBooking(context.Background(), "bkg-1")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return prod.count("slot.released") == 1 }, time.Second, 10*time.Millisecond)

	resp, err := uc.CancelBooking(context.Background(), "bkg-1")

	require.NoError(t, err)
	assert.Equal(t, "CANCELLED", resp.Status)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, prod.count("slot.released"), "cancel kedua tidak boleh publish ulang")
}

func TestCancelBooking_ShouldReject_WhenNotCancellable(t *testing.T) {
	tests := []struct {
		name    string
		status  entity.BookingStatus
		payment string
		code    apperror.ErrorCode
	}{
		{"paid", entity.BookingStatusPaid, "", apperror.ErrBookingPaid},
		{"expired", entity.BookingStatusExpired, "", apperror.ErrBookingExpired},
		{"payment success, booking belum PAID", entity.BookingStatusHeld, "SUCCESS", apperror.ErrBookingPaid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newMockBookingRepo()
			heldBooking(repo, "bkg-1")
			b := repo.bookings["bkg-1"]
			b.Status = tt.status
			repo.bookings["bkg-1"] = b
			repo.payments = map[string]string{"bkg-1": tt.payment}
			prod := newMockProducer()
			cache := &mockCache{}
			_ = cache.StoreBookingQueueToken(context.Background(), "bkg-1", "q_token", time.Minute)
			uc := newCancelUC(repo, cache, prod)

			_, err := uc.CancelBooking(context.Background(), "bkg-1")

			var appErr *apperror.AppError
			require.ErrorAs(t, err, &appErr)
			assert.Equal(t, tt.code, appErr.Code)
			revoked, _ := cache.IsQueueTokenRevoked(context.Background(), "q_token")
			assert.False(t, revoked, "token tidak boleh dicabut bila cancel ditolak")
			time.Sleep(30 * time.Millisecond)
			assert.Equal(t, 0, prod.count("slot.released"))
		})
	}
}

func TestCancelBooking_ShouldReturnNotFound_WhenUnknownBooking(t *testing.T) {
	uc := newCancelUC(newMockBookingRepo(), &mockCache{}, newMockProducer())
	_, err := uc.CancelBooking(context.Background(), "nope")
	var appErr *apperror.AppError
	require.ErrorAs(t, err, &appErr)
	assert.Equal(t, apperror.ErrNotFound, appErr.Code)
}

func TestCreateBooking_ShouldStoreQueueToken_ThenRejectAfterRevoked(t *testing.T) {
	repo := newMockBookingRepo()
	stock := newMockStockRepo(map[string]int{"cat-vip": 10}, map[string]int64{"cat-vip": 1000})
	cache := &mockCache{}
	uc := usecase.NewBookingUsecase(nil, repo, stock, &mockDukcapil{}, cache, newMockProducer(),
		&mockEventClient{}, &mockPaymentTokenSigner{}, slog.Default())
	req := buildRequest("evt-1", "u@test.com", "1234567890123456", "cat-vip")
	req.QueueToken = "q_token"

	resp, err := uc.CreateBooking(context.Background(), req)
	require.NoError(t, err)
	stored, _ := cache.GetBookingQueueToken(context.Background(), resp.BookingID)
	assert.Equal(t, "q_token", stored)

	_, err = uc.CancelBooking(context.Background(), resp.BookingID)
	require.NoError(t, err)

	_, err = uc.CreateBooking(context.Background(), req)
	var appErr *apperror.AppError
	require.ErrorAs(t, err, &appErr)
	assert.Equal(t, apperror.ErrTokenExpired, appErr.Code, "token yang dicabut tidak boleh membuat booking lagi")
}

// notHeldRepo: ExpireBookingTx melaporkan booking sudah tidak HELD (mis. keburu PAID).
type notHeldRepo struct{ *mockBookingRepo }

func (r *notHeldRepo) ExpireBookingTx(_ context.Context, _ string) ([]entity.BookingItem, error) {
	return nil, outbound.ErrBookingNotHeld
}

func TestSweepOnce_ShouldNotPublish_WhenBookingNoLongerHeld(t *testing.T) {
	repo := newMockBookingRepo()
	past := time.Now().Add(-5 * time.Minute)
	repo.bookings["bkg-1"] = entity.Booking{ID: "bkg-1", EventID: "evt-1", Status: entity.BookingStatusHeld, PaymentExpiresAt: &past}
	prod := newMockProducer()
	stock := newMockStockRepo(map[string]int{"cat-1": 5}, map[string]int64{})
	uc := usecase.NewBookingUsecase(nil, &notHeldRepo{repo}, stock, &mockDukcapil{}, &mockCache{}, prod,
		&mockEventClient{}, &mockPaymentTokenSigner{}, slog.Default())

	uc.SweepOnce(context.Background())

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, prod.count("slot.released"))
	assert.Equal(t, 0, prod.count("stock.changed"))
}

// fakeCanceller mencatat order yang di-expire di gateway.
type fakeCanceller struct {
	mu     sync.Mutex
	orders []string
}

func (f *fakeCanceller) ExpireTransaction(_ context.Context, orderID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.orders = append(f.orders, orderID)
	return nil
}

func (f *fakeCanceller) calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.orders...)
}

func TestCancelBooking_ShouldCancelAndExpireMidtrans_WhenPaymentPending(t *testing.T) {
	repo := newMockBookingRepo()
	heldBooking(repo, "bkg-1")
	repo.payments = map[string]string{"bkg-1": "PENDING"}
	prod := newMockProducer()
	uc := newCancelUC(repo, &mockCache{}, prod)
	canceller := &fakeCanceller{}
	uc.SetPaymentCanceller(canceller)

	resp, err := uc.CancelBooking(context.Background(), "bkg-1")

	require.NoError(t, err)
	assert.Equal(t, "CANCELLED", resp.Status)
	assert.Equal(t, "EXPIRED", repo.payments["bkg-1"], "payment PENDING harus ikut di-expire")
	assert.Eventually(t, func() bool { return len(canceller.calls()) == 1 }, time.Second, 10*time.Millisecond)
	assert.Equal(t, []string{"bkg-1"}, canceller.calls())
	assert.Eventually(t, func() bool { return prod.count("slot.released") == 1 }, time.Second, 10*time.Millisecond)
}

func TestCancelBooking_ShouldNotCallMidtrans_WhenNoPendingPayment(t *testing.T) {
	repo := newMockBookingRepo()
	heldBooking(repo, "bkg-1")
	uc := newCancelUC(repo, &mockCache{}, newMockProducer())
	canceller := &fakeCanceller{}
	uc.SetPaymentCanceller(canceller)

	_, err := uc.CancelBooking(context.Background(), "bkg-1")

	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	assert.Empty(t, canceller.calls())
}
