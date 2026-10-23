package usecase_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
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

// ============================================================
// Mock infrastructure
// ============================================================

// mockBookingRepo
type mockBookingRepo struct {
	mu       sync.Mutex
	bookings map[string]entity.Booking
	items    map[string][]entity.BookingItem // bookingId → items
}

func newMockBookingRepo() *mockBookingRepo {
	return &mockBookingRepo{
		bookings: make(map[string]entity.Booking),
		items:    make(map[string][]entity.BookingItem),
	}
}

func (r *mockBookingRepo) Create(_ context.Context, _ outbound.Tx, b entity.Booking, items []entity.BookingItem) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bookings[b.ID] = b
	r.items[b.ID] = items
	return nil
}

// CreateBookingTx runs fn without a real DB transaction (mock).
func (r *mockBookingRepo) CreateBookingTx(
	ctx context.Context,
	_ dto.CreateBookingRequest,
	fn func(context.Context, outbound.Tx) (dto.CreateBookingResponse, error),
) (dto.CreateBookingResponse, error) {
	// Pass nil as Tx — mock repo methods don't need it.
	return fn(ctx, nil)
}

func (r *mockBookingRepo) GetByID(_ context.Context, id string) (entity.Booking, []entity.BookingItem, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.bookings[id]
	if !ok {
		return entity.Booking{}, nil, apperror.NotFound("booking")
	}
	return b, r.items[id], nil
}

func (r *mockBookingRepo) UpdateStatus(_ context.Context, _ outbound.Tx, id string, status entity.BookingStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	b := r.bookings[id]
	b.Status = status
	r.bookings[id] = b
	return nil
}

// ListExpiredHeld mengembalikan booking HELD yang TTL-nya habis.
func (r *mockBookingRepo) ListExpiredHeld(_ context.Context, limit int) ([]entity.Booking, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result []entity.Booking
	for _, b := range r.bookings {
		if b.Status == entity.BookingStatusHeld && b.PaymentExpiresAt != nil && time.Now().After(*b.PaymentExpiresAt) {
			result = append(result, b)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

// HasActiveHeld — no active held bookings in tests (simplification).
func (r *mockBookingRepo) HasActiveHeld(_ context.Context, _ string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, b := range r.bookings {
		if b.Status == entity.BookingStatusHeld {
			return true, nil
		}
	}
	return false, nil
}

// mockStockRepo — implements atomic stock with mutex (simulates DB behavior)
type mockStockRepo struct {
	mu          sync.Mutex
	available   map[string]int   // categoryId → available
	prices      map[string]int64 // categoryId → price
	nikBookings map[string]int   // eventId+nik → count
}

func newMockStockRepo(available map[string]int, prices map[string]int64) *mockStockRepo {
	return &mockStockRepo{
		available:   available,
		prices:      prices,
		nikBookings: make(map[string]int),
	}
}

func (r *mockStockRepo) HoldStock(_ context.Context, _ outbound.Tx, items []entity.HoldItem) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Check semua item dulu sebelum update (simulasi atomic transaction)
	for _, item := range items {
		if r.available[item.CategoryID] < item.Qty {
			return apperror.StockUnavailable()
		}
	}
	// Update semua
	for _, item := range items {
		r.available[item.CategoryID] -= item.Qty
	}
	return nil
}

func (r *mockStockRepo) ReleaseStock(_ context.Context, _ outbound.Tx, bookingID string) error {
	// Simplified: tidak dilacak di mock ini
	return nil
}

func (r *mockStockRepo) GetAvailableByEvent(_ context.Context, eventID string) ([]entity.CategoryStock, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result []entity.CategoryStock
	for id, avail := range r.available {
		result = append(result, entity.CategoryStock{CategoryID: id, Available: avail})
	}
	return result, nil
}

func (r *mockStockRepo) GetPriceMap(_ context.Context, _ string) (map[string]int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := make(map[string]int64, len(r.prices))
	for k, v := range r.prices {
		m[k] = v
	}
	return m, nil
}

func (r *mockStockRepo) CountNIKBookings(_ context.Context, _ outbound.Tx, eventID, nik string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := eventID + ":" + nik
	return r.nikBookings[key], nil
}

func (r *mockStockRepo) incrementNIK(eventID, nik string, qty int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nikBookings[eventID+":"+nik] += qty
}

// mockDukcapil — always valid
type mockDukcapil struct{}

func (m *mockDukcapil) Verify(_ context.Context, _, _, _, _ string) (bool, error) { return true, nil }

// mockCache
type mockCache struct{}

func (m *mockCache) ValidateAccessToken(_ context.Context, _ string) (*outbound.AccessTokenClaims, error) {
	return &outbound.AccessTokenClaims{EventID: "evt-1", BookingTTLExpiresAt: time.Now().Add(5 * time.Minute).Unix()}, nil
}
func (m *mockCache) PublishStockChanged(_ context.Context, _ string, _ []byte) error { return nil }
func (m *mockCache) SubscribeStockChanged(_ context.Context, _ string) (<-chan []byte, func(), error) {
	ch := make(chan []byte)
	return ch, func() { close(ch) }, nil
}

// mockProducer — no-op, counts publishes
type mockProducer struct {
	mu       sync.Mutex
	publishes map[string]int
}

func newMockProducer() *mockProducer {
	return &mockProducer{publishes: make(map[string]int)}
}

func (p *mockProducer) publish(topic string) {
	p.mu.Lock()
	p.publishes[topic]++
	p.mu.Unlock()
}

func (p *mockProducer) count(topic string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.publishes[topic]
}

func (p *mockProducer) PublishBookingCreated(_ context.Context, _ string, _ any) error {
	p.publish("booking.created")
	return nil
}

func (p *mockProducer) PublishStockChanged(_ context.Context, _ string, _ any) error {
	p.publish("stock.changed")
	return nil
}

func (p *mockProducer) PublishSlotReleased(_ context.Context, _ string, _ any) error {
	p.publish("slot.released")
	return nil
}

func (p *mockProducer) PublishEventSoldOut(_ context.Context, _ string, _ any) error {
	p.publish("event.sold_out")
	return nil
}

// ============================================================
// Helpers
// ============================================================

func buildRequest(eventID, email, nik, categoryID string) dto.CreateBookingRequest {
	return dto.CreateBookingRequest{
		EventID:             eventID,
		Email:               email,
		EventIDFromToken:    eventID,
		BookingTTLExpiresAt: time.Now().Add(5 * time.Minute).Unix(),
		Items: []dto.BookingItem{
			{
				CategoryID: categoryID,
				Attendees: []dto.AttendeeInput{
					{NIK: nik, Nama: "Test User", TglLahir: "1990-01-01", NamaIbu: "Ibu Test"},
				},
			},
		},
	}
}

// mockEventClient — no-op event service client
type mockEventClient struct{}

func (m *mockEventClient) MarkSoldOut(_ context.Context, _ string) error { return nil }

func newUC(bookingRepo *mockBookingRepo, stockRepo *mockStockRepo, prod *mockProducer) *usecase.BookingUsecase {
	return usecase.NewBookingUsecase(
		nil, // pool nil — sweeper tidak ditest di sini; CreateBooking tidak pakai pool langsung
		bookingRepo, stockRepo,
		&mockDukcapil{}, &mockCache{}, prod,
		&mockEventClient{},
		slog.Default(),
	)
}

// ============================================================
// Tests
// ============================================================

func TestCreateBooking_HappyPath(t *testing.T) {
	bookingRepo := newMockBookingRepo()
	stockRepo := newMockStockRepo(
		map[string]int{"cat-vip": 100},
		map[string]int64{"cat-vip": 500000},
	)
	prod := newMockProducer()
	uc := newUC(bookingRepo, stockRepo, prod)

	resp, err := uc.CreateBooking(context.Background(), buildRequest("evt-1", "user@test.com", "1234567890123456", "cat-vip"))
	require.NoError(t, err)
	assert.Equal(t, "HELD", resp.Status)
	assert.Equal(t, int64(500000), resp.Total)
	assert.NotEmpty(t, resp.BookingID)

	// Available harus berkurang 1
	assert.Equal(t, 99, stockRepo.available["cat-vip"])
}

// TestAntiOversell_Concurrent adalah test WAJIB untuk sidang.
// N goroutine berebut stok terakhir — hanya 1 yang berhasil.
func TestAntiOversell_Concurrent(t *testing.T) {
	const totalStock = 1
	const goroutines = 10

	bookingRepo := newMockBookingRepo()
	stockRepo := newMockStockRepo(
		map[string]int{"cat-vip": totalStock},
		map[string]int64{"cat-vip": 500000},
	)
	prod := newMockProducer()
	uc := newUC(bookingRepo, stockRepo, prod)

	var successCount atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			nik := generateNIK(idx)
			_, err := uc.CreateBooking(context.Background(),
				buildRequest("evt-1", "user@test.com", nik, "cat-vip"))
			if err == nil {
				successCount.Add(1)
			} else {
				var appErr *apperror.AppError
				assert.True(t, errors.As(err, &appErr),
					"error harus AppError, bukan: %v", err)
			}
		}(i)
	}

	wg.Wait()

	// HANYA 1 yang berhasil (sesuai stok = 1)
	assert.Equal(t, int32(totalStock), successCount.Load(),
		"tepat %d booking sukses, sesuai stok tersedia", totalStock)

	// available tidak pernah < 0
	assert.GreaterOrEqual(t, stockRepo.available["cat-vip"], 0,
		"available tidak boleh negatif")

	// Tepat 1 booking ada di repo
	bookingRepo.mu.Lock()
	bookingCount := len(bookingRepo.bookings)
	bookingRepo.mu.Unlock()
	assert.Equal(t, totalStock, bookingCount)
}

// TestAntiOversell_Stock1_Two_Concurrent — stok 1, 2 user bersamaan → 1 HELD, 1 gagal.
func TestAntiOversell_Stock1_Two_Concurrent(t *testing.T) {
	bookingRepo := newMockBookingRepo()
	stockRepo := newMockStockRepo(
		map[string]int{"cat-vip": 1},
		map[string]int64{"cat-vip": 500000},
	)
	uc := newUC(bookingRepo, stockRepo, newMockProducer())

	results := make([]error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			nik := generateNIK(idx)
			_, err := uc.CreateBooking(context.Background(),
				buildRequest("evt-1", "user@test.com", nik, "cat-vip"))
			results[idx] = err
		}(i)
	}
	wg.Wait()

	successes := 0
	for _, err := range results {
		if err == nil {
			successes++
		}
	}
	assert.Equal(t, 1, successes, "tepat 1 booking sukses saat stok=1 dan 2 user bersamaan")
	assert.Equal(t, 0, stockRepo.available["cat-vip"], "available harus 0 setelah 1 hold")
}

// TestQuotaNIK_ExceedLimit — satu NIK tidak bisa booking lebih dari 3 tiket.
func TestQuotaNIK_ExceedLimit(t *testing.T) {
	const nik = "1234567890123456"
	const eventID = "evt-1"

	bookingRepo := newMockBookingRepo()
	stockRepo := newMockStockRepo(
		map[string]int{"cat-vip": 10},
		map[string]int64{"cat-vip": 500000},
	)
	// Simulasi NIK sudah punya 3 tiket aktif
	stockRepo.nikBookings[eventID+":"+nik] = 3

	uc := newUC(bookingRepo, stockRepo, newMockProducer())

	_, err := uc.CreateBooking(context.Background(), buildRequest(eventID, "user@test.com", nik, "cat-vip"))
	require.Error(t, err)
	var appErr *apperror.AppError
	require.True(t, errors.As(err, &appErr))
	assert.Equal(t, apperror.ErrQuotaExceeded, appErr.Code)
}

// TestMaxTicketsPerBooking — lebih dari 3 tiket dalam satu booking ditolak.
func TestMaxTicketsPerBooking(t *testing.T) {
	bookingRepo := newMockBookingRepo()
	stockRepo := newMockStockRepo(
		map[string]int{"cat-vip": 10},
		map[string]int64{"cat-vip": 500000},
	)
	uc := newUC(bookingRepo, stockRepo, newMockProducer())

	req := dto.CreateBookingRequest{
		EventID:             "evt-1",
		Email:               "user@test.com",
		EventIDFromToken:    "evt-1",
		BookingTTLExpiresAt: time.Now().Add(5 * time.Minute).Unix(),
		Items: []dto.BookingItem{
			{
				CategoryID: "cat-vip",
				Attendees: []dto.AttendeeInput{
					{NIK: "1234567890123451", Nama: "A", TglLahir: "1990-01-01", NamaIbu: "B"},
					{NIK: "1234567890123452", Nama: "C", TglLahir: "1990-01-01", NamaIbu: "D"},
					{NIK: "1234567890123453", Nama: "E", TglLahir: "1990-01-01", NamaIbu: "F"},
					{NIK: "1234567890123454", Nama: "G", TglLahir: "1990-01-01", NamaIbu: "H"}, // ke-4 = tolak
				},
			},
		},
	}

	_, err := uc.CreateBooking(context.Background(), req)
	require.Error(t, err)
	var appErr *apperror.AppError
	require.True(t, errors.As(err, &appErr))
	assert.Equal(t, apperror.ErrValidation, appErr.Code)
}

// TestEventIDMismatch — eventId di body berbeda dari Access Token claim.
func TestEventIDMismatch(t *testing.T) {
	bookingRepo := newMockBookingRepo()
	stockRepo := newMockStockRepo(map[string]int{"cat-vip": 10}, map[string]int64{"cat-vip": 500000})
	uc := newUC(bookingRepo, stockRepo, newMockProducer())

	req := buildRequest("evt-1", "user@test.com", "1234567890123456", "cat-vip")
	req.EventIDFromToken = "evt-LAIN" // mismatch

	_, err := uc.CreateBooking(context.Background(), req)
	require.Error(t, err)
	var appErr *apperror.AppError
	require.True(t, errors.As(err, &appErr))
	assert.Equal(t, apperror.ErrTokenInvalid, appErr.Code)
}

// TestPublishAfterCommit — setelah booking sukses, booking.created & stock.changed dipublish.
func TestPublishAfterCommit(t *testing.T) {
	bookingRepo := newMockBookingRepo()
	stockRepo := newMockStockRepo(
		map[string]int{"cat-vip": 10},
		map[string]int64{"cat-vip": 500000},
	)
	prod := newMockProducer()
	uc := newUC(bookingRepo, stockRepo, prod)

	_, err := uc.CreateBooking(context.Background(), buildRequest("evt-1", "user@test.com", "1234567890123456", "cat-vip"))
	require.NoError(t, err)

	// Beri waktu goroutine publish berjalan (best-effort async)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, prod.count("booking.created"), "booking.created harus dipublish 1x")
	assert.GreaterOrEqual(t, prod.count("stock.changed"), 1, "stock.changed harus dipublish")
}

// ============================================================
// Helpers
// ============================================================

func generateNIK(idx int) string {
	base := "123456789012345"
	return base + string(rune('0'+idx%10))
}

// Pastikan mock mengimplementasikan interface (compile-time check).
var _ outbound.IBookingRepository = (*mockBookingRepo)(nil)
var _ outbound.IStockRepository = (*mockStockRepo)(nil)
var _ outbound.IDukcapilClient = (*mockDukcapil)(nil)
var _ outbound.ICacheRepository = (*mockCache)(nil)
var _ outbound.IEventProducer = (*mockProducer)(nil)
var _ outbound.IEventServiceClient = (*mockEventClient)(nil)
