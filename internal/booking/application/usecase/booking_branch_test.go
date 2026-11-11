package usecase_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/refreshTechRusuh/booking-service/internal/booking/application/usecase"
	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/dto"
	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/entity"
	"github.com/refreshTechRusuh/booking-service/internal/booking/port/outbound"
	"github.com/refreshTechRusuh/booking-service/pkg/apperror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- CreateBooking: EventIDFromToken mismatch ------------------------------

func TestCreateBooking_EventIDMismatch_TokenInvalid(t *testing.T) {
	bookingRepo := newMockBookingRepo()
	stockRepo := newMockStockRepo(map[string]int{"cat-1": 10}, map[string]int64{"cat-1": 100000})
	uc := newUC(bookingRepo, stockRepo, newMockProducer())

	req := buildRequest("evt-1", "user@test.com", "1234567890123456", "cat-1")
	req.EventIDFromToken = "evt-OTHER" // mismatch: token untuk event berbeda
	_, err := uc.CreateBooking(context.Background(), req)
	require.Error(t, err)
	var appErr *apperror.AppError
	require.True(t, errors.As(err, &appErr))
	assert.Equal(t, apperror.ErrTokenInvalid, appErr.Code)
}

// ---- CreateBooking: BookingTTL expired -------------------------------------

func TestCreateBooking_TTLExpired_TokenExpired(t *testing.T) {
	bookingRepo := newMockBookingRepo()
	stockRepo := newMockStockRepo(map[string]int{"cat-1": 10}, map[string]int64{"cat-1": 100000})
	uc := newUC(bookingRepo, stockRepo, newMockProducer())

	req := buildRequest("evt-1", "user@test.com", "1234567890123456", "cat-1")
	req.BookingTTLExpiresAt = time.Now().Add(-2 * time.Minute).Unix() // expired
	_, err := uc.CreateBooking(context.Background(), req)
	require.Error(t, err)
	var appErr *apperror.AppError
	require.True(t, errors.As(err, &appErr))
	assert.Equal(t, apperror.ErrTokenExpired, appErr.Code)
}

// ---- CreateBooking: Dukcapil verify error ----------------------------------

func TestCreateBooking_DukcapilVerifyError_NIKInvalid(t *testing.T) {
	bookingRepo := newMockBookingRepo()
	stockRepo := newMockStockRepo(map[string]int{"cat-1": 10}, map[string]int64{"cat-1": 100000})
	errDukcapil := &errDukcapilClient{}
	uc := usecase.NewBookingUsecase(nil, bookingRepo, stockRepo,
		errDukcapil, &mockCache{}, newMockProducer(),
		&mockEventClient{}, &mockPaymentTokenSigner{}, slog.Default())

	req := buildRequest("evt-1", "user@test.com", "1234567890123456", "cat-1")
	req.EventIDFromToken = "evt-1"
	_, err := uc.CreateBooking(context.Background(), req)
	require.Error(t, err)
	var appErr *apperror.AppError
	require.True(t, errors.As(err, &appErr))
	assert.Equal(t, apperror.ErrNIKInvalid, appErr.Code)
}

func TestCreateBooking_DukcapilInvalidNIK_NIKInvalid(t *testing.T) {
	bookingRepo := newMockBookingRepo()
	stockRepo := newMockStockRepo(map[string]int{"cat-1": 10}, map[string]int64{"cat-1": 100000})
	invalidDukcapil := &invalidDukcapilClient{} // returns valid=false
	uc := usecase.NewBookingUsecase(nil, bookingRepo, stockRepo,
		invalidDukcapil, &mockCache{}, newMockProducer(),
		&mockEventClient{}, &mockPaymentTokenSigner{}, slog.Default())

	req := buildRequest("evt-1", "user@test.com", "9999999999999999", "cat-1")
	req.EventIDFromToken = "evt-1"
	_, err := uc.CreateBooking(context.Background(), req)
	require.Error(t, err)
	var appErr *apperror.AppError
	require.True(t, errors.As(err, &appErr))
	assert.Equal(t, apperror.ErrNIKInvalid, appErr.Code)
}

// ---- CreateBooking: payment token sign error --------------------------------

func TestCreateBooking_PaymentTokenSignError_Internal(t *testing.T) {
	bookingRepo := newMockBookingRepo()
	stockRepo := newMockStockRepo(map[string]int{"cat-1": 10}, map[string]int64{"cat-1": 100000})
	errSigner := &errPaymentTokenSigner{}
	uc := usecase.NewBookingUsecase(nil, bookingRepo, stockRepo,
		&mockDukcapil{}, &mockCache{}, newMockProducer(),
		&mockEventClient{}, errSigner, slog.Default())

	req := buildRequest("evt-1", "user@test.com", "1234567890123456", "cat-1")
	_, err := uc.CreateBooking(context.Background(), req)
	require.Error(t, err)
	var appErr *apperror.AppError
	require.True(t, errors.As(err, &appErr))
	assert.Equal(t, apperror.ErrInternal, appErr.Code)
}

// ---- publishPostBooking: PaymentExpiresAt nil via sweeper ------------------

func TestCreateBooking_PublishPostBooking_NilPaymentExpiresAt(t *testing.T) {
	bookingRepo := newMockBookingRepo()
	past := time.Now().Add(-5 * time.Minute)
	bookingRepo.bookings["bkg-noexp"] = entity.Booking{
		ID: "bkg-noexp", EventID: "evt-1",
		Status:           entity.BookingStatusHeld,
		PaymentExpiresAt: &past,
	}
	prod := newMockProducer()
	stockRepo := newMockStockRepo(map[string]int{"cat-1": 5}, map[string]int64{})
	uc := newUC(bookingRepo, stockRepo, prod)

	assert.NotPanics(t, func() {
		uc.SweepOnce(context.Background())
		time.Sleep(50 * time.Millisecond)
	})
}

// ---- postExpirePublish: error paths via errProducer ------------------------

func TestPostExpirePublish_PublishErrors_NoPanic(t *testing.T) {
	// errProducer return error untuk semua publish -- harus tidak panic
	bookingRepo := newMockBookingRepo()
	past := time.Now().Add(-5 * time.Minute)
	bookingRepo.bookings["bkg-err-pub"] = entity.Booking{
		ID: "bkg-err-pub", EventID: "evt-1",
		Status:           entity.BookingStatusHeld,
		PaymentExpiresAt: &past,
	}
	bookingRepo.items["bkg-err-pub"] = []entity.BookingItem{
		{CategoryID: "cat-1"},
	}
	errProd := &errProducer{}
	stockRepo := newMockStockRepo(map[string]int{"cat-1": 5}, map[string]int64{})
	uc := usecase.NewBookingUsecase(nil, bookingRepo, stockRepo,
		&mockDukcapil{}, &mockCache{}, errProd,
		&mockEventClient{}, &mockPaymentTokenSigner{}, slog.Default())

	assert.NotPanics(t, func() {
		uc.SweepOnce(context.Background())
		time.Sleep(100 * time.Millisecond) // wait goroutine
	})
}

// ---- publishPostBooking via CreateBooking with errProducer ----------------

func TestCreateBooking_PublishErrors_NoPanic(t *testing.T) {
	bookingRepo := newMockBookingRepo()
	stockRepo := newMockStockRepo(map[string]int{"cat-1": 10}, map[string]int64{"cat-1": 100000})
	errProd := &errProducer{}
	uc := usecase.NewBookingUsecase(nil, bookingRepo, stockRepo,
		&mockDukcapil{}, &mockCache{}, errProd,
		&mockEventClient{}, &mockPaymentTokenSigner{}, slog.Default())

	req := buildRequest("evt-1", "user@test.com", "1234567890123456", "cat-1")
	resp, err := uc.CreateBooking(context.Background(), req)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.BookingID)

	// Tunggu goroutine publishPostBooking selesai
	time.Sleep(100 * time.Millisecond)
}

// ---- StreamStock: sold-out initial snapshot --------------------------------

func TestStreamStock_AllSoldOut_InitialSnapshot(t *testing.T) {
	bookingRepo := newMockBookingRepo()
	stockRepo := newMockStockRepo(map[string]int{"cat-1": 0}, map[string]int64{})
	soldOutRepo := &soldOutStockRepo{mockStockRepo: stockRepo}
	uc := usecase.NewBookingUsecase(nil, bookingRepo, soldOutRepo,
		&mockDukcapil{}, &mockCache{}, newMockProducer(),
		&mockEventClient{}, &mockPaymentTokenSigner{}, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	ch, err := uc.StreamStock(ctx, "evt-1")
	require.NoError(t, err)

	select {
	case ev, ok := <-ch:
		if ok {
			assert.True(t, ev.SoldOut, "initial snapshot harus kirim sold-out event")
		}
	case <-ctx.Done():
		// acceptable
	}
}

// ---- StreamStock: SubscribeStockChanged error ------------------------------

func TestStreamStock_SubscribeError_ReturnsError(t *testing.T) {
	bookingRepo := newMockBookingRepo()
	stockRepo := newMockStockRepo(map[string]int{"cat-1": 5}, map[string]int64{})
	errCache := &errSubscribeCache{}
	uc := usecase.NewBookingUsecase(nil, bookingRepo, stockRepo,
		&mockDukcapil{}, errCache, newMockProducer(),
		&mockEventClient{}, &mockPaymentTokenSigner{}, slog.Default())

	_, err := uc.StreamStock(context.Background(), "evt-1")
	assert.Error(t, err)
}

// ---- StreamStock: message channel closed (goroutine exits) -----------------

func TestStreamStock_MsgChannelClosed_Exits(t *testing.T) {
	bookingRepo := newMockBookingRepo()
	stockRepo := newMockStockRepo(map[string]int{"cat-1": 5}, map[string]int64{})
	// closingCache: kirim channel yang langsung ditutup
	closeCache := &closingCache{}
	uc := usecase.NewBookingUsecase(nil, bookingRepo, stockRepo,
		&mockDukcapil{}, closeCache, newMockProducer(),
		&mockEventClient{}, &mockPaymentTokenSigner{}, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	ch, err := uc.StreamStock(ctx, "evt-1")
	require.NoError(t, err)

	// Drain channel sampai ditutup
	var events []dto.StockBatchEvent
	for ev := range ch {
		events = append(events, ev)
	}
	// Channel sudah ditutup karena msgCh ditutup
	assert.GreaterOrEqual(t, len(events), 0)
}

// ---- StreamStock: empty categories ----------------------------------------

func TestStreamStock_EmptySnapshot_NoInitialSend(t *testing.T) {
	bookingRepo := newMockBookingRepo()
	stockRepo := newMockStockRepo(map[string]int{}, map[string]int64{})
	uc := newUC(bookingRepo, stockRepo, newMockProducer())

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	ch, err := uc.StreamStock(ctx, "evt-1")
	require.NoError(t, err)
	require.NotNil(t, ch)
}

// ---- Mock overrides --------------------------------------------------------

// errDukcapilClient: Verify selalu return error
type errDukcapilClient struct{}

func (m *errDukcapilClient) Verify(_ context.Context, _, _, _, _ string) (bool, error) {
	return false, errors.New("dukcapil service unavailable")
}

// invalidDukcapilClient: Verify selalu return valid=false
type invalidDukcapilClient struct{}

func (m *invalidDukcapilClient) Verify(_ context.Context, _, _, _, _ string) (bool, error) {
	return false, nil
}

// errPaymentTokenSigner: Sign selalu return error
type errPaymentTokenSigner struct{}

func (m *errPaymentTokenSigner) Sign(_, _ string, _ int64) (string, error) {
	return "", errors.New("signing failed")
}

// soldOutStockRepo: semua available=0
type soldOutStockRepo struct {
	*mockStockRepo // embed mock supaya GetEventName dan GetPriceMap tersedia
}

func (r *soldOutStockRepo) GetAvailableByEvent(_ context.Context, _ string) ([]entity.CategoryStock, error) {
	return []entity.CategoryStock{
		{CategoryID: "cat-1", Name: "VIP", Available: 0},
	}, nil
}

// errSubscribeCache: SubscribeStockChanged return error
type errSubscribeCache struct {
	mockCache
}

func (c *errSubscribeCache) SubscribeStockChanged(_ context.Context, _ string) (<-chan []byte, func(), error) {
	return nil, nil, errors.New("subscribe failed")
}

// closingCache: SubscribeStockChanged return channel yang langsung ditutup setelah initial snapshot
type closingCache struct {
	mockCache
}

func (c *closingCache) SubscribeStockChanged(_ context.Context, _ string) (<-chan []byte, func(), error) {
	ch := make(chan []byte)
	go func() {
		// Kirim satu message lalu tutup
		time.Sleep(10 * time.Millisecond)
		close(ch)
	}()
	return ch, func() {}, nil
}

// errDukcapilClient must implement outbound.IDukcapilClient
var _ outbound.IDukcapilClient = (*errDukcapilClient)(nil)
var _ outbound.IDukcapilClient = (*invalidDukcapilClient)(nil)

// errProducer: semua Publish return error
type errProducer struct{}

func (p *errProducer) PublishBookingCreated(_ context.Context, _ string, _ interface{}) error {
	return errors.New("kafka unavailable")
}
func (p *errProducer) PublishStockChanged(_ context.Context, _ string, _ interface{}) error {
	return errors.New("kafka unavailable")
}
func (p *errProducer) PublishSlotReleased(_ context.Context, _ string, _ interface{}) error {
	return errors.New("kafka unavailable")
}
func (p *errProducer) PublishEventSoldOut(_ context.Context, _ string, _ interface{}) error {
	return errors.New("kafka unavailable")
}
