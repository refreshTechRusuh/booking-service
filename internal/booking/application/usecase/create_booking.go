// Package usecase mengimplementasikan logika bisnis booking-service.
package usecase

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/dto"
	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/entity"
	"github.com/refreshTechRusuh/booking-service/internal/booking/port/outbound"
	"github.com/refreshTechRusuh/booking-service/pkg/apperror"
)

const (
	maxTicketsPerBooking = 3
	maxTicketsPerNIK     = 3
	bookingTTLMinutes    = 10
	backDisabledThreshold = 30 // seconds
)

// BookingUsecase mengimplementasikan inbound.IBookingUsecase.
type BookingUsecase struct {
	pool      *pgxpool.Pool
	bookingRepo outbound.IBookingRepository
	stockRepo   outbound.IStockRepository
	dukcapil    outbound.IDukcapilClient
	cache       outbound.ICacheRepository
	producer    outbound.IEventProducer
	log         *slog.Logger
}

func NewBookingUsecase(
	pool *pgxpool.Pool,
	bookingRepo outbound.IBookingRepository,
	stockRepo outbound.IStockRepository,
	dukcapil outbound.IDukcapilClient,
	cache outbound.ICacheRepository,
	producer outbound.IEventProducer,
	log *slog.Logger,
) *BookingUsecase {
	return &BookingUsecase{
		pool:        pool,
		bookingRepo: bookingRepo,
		stockRepo:   stockRepo,
		dukcapil:    dukcapil,
		cache:       cache,
		producer:    producer,
		log:         log,
	}
}

// CreateBooking mengeksekusi 6 langkah validasi + hold stok dalam satu transaksi pgx.
func (u *BookingUsecase) CreateBooking(ctx context.Context, req dto.CreateBookingRequest) (dto.CreateBookingResponse, error) {
	// ── 1. Validasi Access Token (sudah dilakukan di middleware, ambil claims dari ctx)
	// Access Token diverifikasi di RequireAccessToken middleware sebelum request masuk ke sini.
	// Claims sudah ada di context — usecase tinggal pakai req.EventIDFromToken, req.BookingTTLExpiresAt.

	// ── 2. Validasi email ada (dilakukan validator di handler)

	// ── 3. Validasi format NIK (len=16) dilakukan validator di handler

	// ── 4-6. Jalankan dalam satu transaksi pgx
	tx, err := u.pool.Begin(ctx)
	if err != nil {
		return dto.CreateBookingResponse{}, apperror.Internal(fmt.Errorf("begin tx: %w", err))
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// ── 4. Verifikasi Dukcapil per attendee (defense-in-depth, tanpa OTP)
	for _, item := range req.Items {
		for _, a := range item.Attendees {
			valid, err := u.dukcapil.Verify(ctx, a.NIK, a.Nama, a.TglLahir, a.NamaIbu)
			if err != nil {
				u.log.WarnContext(ctx, "dukcapil verify error (fail-open behavior applies)", "nik", a.NIK, "err", err)
				// failOpen dikontrol di DukcapilClient — jika err masih ada berarti fail-closed
				return dto.CreateBookingResponse{}, apperror.NIKInvalid(a.NIK)
			}
			if !valid {
				return dto.CreateBookingResponse{}, apperror.NIKInvalid(a.NIK)
			}
		}
	}

	// ── 5. Total tiket ≤ 3 per booking
	if req.TotalTickets() > maxTicketsPerBooking {
		return dto.CreateBookingResponse{}, apperror.New(apperror.ErrValidation,
			fmt.Sprintf("total tiket maksimal %d per booking", maxTicketsPerBooking))
	}

	// ── 6a. Kuota per-NIK: hitung tiket HELD/PAID per (eventId, nik) dengan FOR UPDATE
	nikQtyMap := req.NIKQtyMap()
	for nik, qtyThisBooking := range nikQtyMap {
		existing, err := u.stockRepo.CountNIKBookings(ctx, tx, req.EventID, nik)
		if err != nil {
			return dto.CreateBookingResponse{}, apperror.Internal(err)
		}
		if existing+qtyThisBooking > maxTicketsPerNIK {
			return dto.CreateBookingResponse{}, apperror.QuotaExceeded()
		}
	}

	// ── 6b. Hold stok atomic — satu UPDATE per kategori, rollback jika gagal
	holdItems := req.ToHoldItems()
	if err := u.stockRepo.HoldStock(ctx, tx, holdItems); err != nil {
		return dto.CreateBookingResponse{}, err // StockUnavailable atau internal
	}

	// ── Simpan booking + booking_items
	now := time.Now()
	bookingExpiresAt := time.Unix(req.BookingTTLExpiresAt, 0)
	paymentExpiresAt := now.Add(bookingTTLMinutes * time.Minute)

	bookingID := newUUID()
	booking := entity.Booking{
		ID:               bookingID,
		UserID:           req.UserID,
		EventID:          req.EventID,
		Email:            req.Email,
		Status:           entity.BookingStatusHeld,
		BookingExpiresAt: &bookingExpiresAt,
		PaymentExpiresAt: &paymentExpiresAt,
		CreatedAt:        now,
	}

	var items []entity.BookingItem
	var total int64
	for _, item := range req.Items {
		for _, a := range item.Attendees {
			price := req.PriceSnapshot(item.CategoryID) // harga saat booking
			items = append(items, entity.BookingItem{
				ID:            newUUID(),
				BookingID:     bookingID,
				CategoryID:    item.CategoryID,
				NIK:           a.NIK,
				PriceSnapshot: price,
			})
			total += price
		}
	}

	if err := u.bookingRepo.Create(ctx, tx, booking, items); err != nil {
		return dto.CreateBookingResponse{}, apperror.Internal(err)
	}

	if err := tx.Commit(ctx); err != nil {
		return dto.CreateBookingResponse{}, apperror.Internal(fmt.Errorf("commit tx: %w", err))
	}

	// ── Publish events (setelah commit — best-effort, tidak rollback booking)
	go u.publishPostBooking(context.WithoutCancel(ctx), booking, items, total)

	return dto.CreateBookingResponse{
		BookingID:        bookingID,
		Status:           string(entity.BookingStatusHeld),
		Total:            total,
		PaymentExpiresAt: paymentExpiresAt.UTC().Format(time.RFC3339),
	}, nil
}

// GetBooking mengembalikan status booking + sisa TTL + backDisabled.
func (u *BookingUsecase) GetBooking(ctx context.Context, bookingID string) (dto.BookingStatusResponse, error) {
	booking, _, err := u.bookingRepo.GetByID(ctx, bookingID)
	if err != nil {
		return dto.BookingStatusResponse{}, apperror.NotFound("booking")
	}

	remaining := 0
	if booking.PaymentExpiresAt != nil {
		remaining = int(time.Until(*booking.PaymentExpiresAt).Seconds())
		if remaining < 0 {
			remaining = 0
		}
	}

	return dto.BookingStatusResponse{
		BookingID:        booking.ID,
		Status:           string(booking.Status),
		RemainingSeconds: remaining,
		BackDisabled:     remaining < backDisabledThreshold,
	}, nil
}

// StreamStock mengembalikan channel event stok dari Redis Pub/Sub.
func (u *BookingUsecase) StreamStock(ctx context.Context, eventID string, categoryIDs []string) (<-chan dto.StockEvent, error) {
	msgCh, unsub, err := u.cache.SubscribeStockChanged(ctx, eventID)
	if err != nil {
		return nil, apperror.Internal(err)
	}

	out := make(chan dto.StockEvent, 16)
	go func() {
		defer unsub()
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				var ev dto.StockEvent
				if err := parseJSON(msg, &ev); err != nil {
					continue
				}
				if len(categoryIDs) > 0 && !containsString(categoryIDs, ev.CategoryID) {
					continue // filter kategori
				}
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out, nil
}

// ---- post-booking side effects (async, non-blocking) -------------------------

func (u *BookingUsecase) publishPostBooking(ctx context.Context, b entity.Booking, items []entity.BookingItem, total int64) {
	// booking.created → Notification Worker (email #1)
	// stock.changed (HOLD) → Queue Service cache stok + SSE broadcast
	// (implementasi lengkap di Task B2-2B.5)
	u.log.InfoContext(ctx, "post booking publish (placeholder)", "bookingId", b.ID)
}

// ---- helpers -----------------------------------------------------------------

func newUUID() string {
	// TODO: ganti dengan uuid library: github.com/google/uuid
	return fmt.Sprintf("bkg-%d", time.Now().UnixNano())
}

func parseJSON(b []byte, v any) error {
	import_encoding_json() // placeholder
	return nil
}

func import_encoding_json() {} // placeholder — hapus, gunakan encoding/json langsung

func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
