package usecase_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/refreshTechRusuh/booking-service/internal/booking/application/usecase"
	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/entity"
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
		payment bool
		code    apperror.ErrorCode
	}{
		{"paid", entity.BookingStatusPaid, false, apperror.ErrBookingPaid},
		{"expired", entity.BookingStatusExpired, false, apperror.ErrBookingExpired},
		{"payment in progress", entity.BookingStatusHeld, true, apperror.ErrPaymentInProgress},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newMockBookingRepo()
			heldBooking(repo, "bkg-1")
			b := repo.bookings["bkg-1"]
			b.Status = tt.status
			repo.bookings["bkg-1"] = b
			repo.payments = map[string]bool{"bkg-1": tt.payment}
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
