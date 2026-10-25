// Package usecase mengimplementasikan logika bisnis booking-service.
package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/dto"
	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/entity"
	"github.com/refreshTechRusuh/booking-service/internal/booking/port/outbound"
	"github.com/refreshTechRusuh/booking-service/pkg/apperror"
	"github.com/refreshTechRusuh/booking-service/pkg/kafka"
)

const (
	maxTicketsPerBooking  = 3
	maxTicketsPerNIK      = 3
	paymentTTLMinutes     = 10
	backDisabledThreshold = 30 // seconds
)

// BookingUsecase mengimplementasikan inbound.IBookingUsecase.
type BookingUsecase struct {
	pool        *pgxpool.Pool
	bookingRepo outbound.IBookingRepository
	stockRepo   outbound.IStockRepository
	dukcapil    outbound.IDukcapilClient
	cache       outbound.ICacheRepository
	producer    outbound.IEventProducer
	eventClient outbound.IEventServiceClient
	log         *slog.Logger
}

func NewBookingUsecase(
	pool *pgxpool.Pool,
	bookingRepo outbound.IBookingRepository,
	stockRepo outbound.IStockRepository,
	dukcapil outbound.IDukcapilClient,
	cache outbound.ICacheRepository,
	producer outbound.IEventProducer,
	eventClient outbound.IEventServiceClient,
	log *slog.Logger,
) *BookingUsecase {
	return &BookingUsecase{
		pool: pool, bookingRepo: bookingRepo, stockRepo: stockRepo,
		dukcapil: dukcapil, cache: cache, producer: producer,
		eventClient: eventClient, log: log,
	}
}

// CreateBooking mengeksekusi 6 langkah validasi + hold stok dalam satu transaksi pgx.
func (u *BookingUsecase) CreateBooking(ctx context.Context, req dto.CreateBookingRequest) (dto.CreateBookingResponse, error) {
	// Validasi: eventId dari Access Token harus cocok dengan eventId di body.
	if req.EventIDFromToken != "" && req.EventIDFromToken != req.EventID {
		return dto.CreateBookingResponse{}, apperror.TokenInvalid()
	}

	// ── 4. Verifikasi Dukcapil per attendee SEBELUM buka transaksi (network call, tidak perlu tx)
	for _, item := range req.Items {
		for _, a := range item.Attendees {
			valid, verifyErr := u.dukcapil.Verify(ctx, a.NIK, a.Nama, a.TglLahir, a.NamaIbu)
			if verifyErr != nil {
				u.log.WarnContext(ctx, "dukcapil verify error",
					slog.String("nik", a.NIK), slog.Any("err", verifyErr))
				return dto.CreateBookingResponse{}, apperror.NIKInvalid(a.NIK)
			}
			if !valid {
				return dto.CreateBookingResponse{}, apperror.NIKInvalid(a.NIK)
			}
		}
	}

	// ── 5. Total tiket ≤ 3 per booking
	if total := req.TotalTickets(); total > maxTicketsPerBooking {
		return dto.CreateBookingResponse{}, apperror.New(apperror.ErrValidation,
			fmt.Sprintf("total tiket maksimal %d per booking, diterima %d", maxTicketsPerBooking, total))
	}

	// ── 6a-6b. Jalankan dalam satu transaksi pgx via repository
	return u.bookingRepo.CreateBookingTx(ctx, req, func(ctx context.Context, tx outbound.Tx) (dto.CreateBookingResponse, error) {
		// 6a. Kuota per-NIK
		for nik, qtyThisBooking := range req.NIKQtyMap() {
			existing, err := u.stockRepo.CountNIKBookings(ctx, tx, req.EventID, nik)
			if err != nil {
				return dto.CreateBookingResponse{}, apperror.Internal(err)
			}
			if existing+qtyThisBooking > maxTicketsPerNIK {
				return dto.CreateBookingResponse{}, apperror.QuotaExceeded()
			}
		}

		// 6b. Hold stok atomic
		holdItems := req.ToHoldItems()
		if err := u.stockRepo.HoldStock(ctx, tx, holdItems); err != nil {
			return dto.CreateBookingResponse{}, err
		}

		// Ambil harga per kategori untuk price_snapshot
		priceMap, err := u.stockRepo.GetPriceMap(ctx, req.EventID)
		if err != nil {
			return dto.CreateBookingResponse{}, apperror.Internal(err)
		}

		// Buat entity booking + items
		now := time.Now()
		bookingExpiresAt := time.Unix(req.BookingTTLExpiresAt, 0)
		if req.BookingTTLExpiresAt == 0 {
			bookingExpiresAt = now.Add(5 * time.Minute)
		}
		paymentExpiresAt := now.Add(paymentTTLMinutes * time.Minute)

		bookingID := uuid.New().String()
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
		var totalAmount int64
		for _, item := range req.Items {
			price := priceMap[item.CategoryID]
			for _, a := range item.Attendees {
				items = append(items, entity.BookingItem{
					ID:            uuid.New().String(),
					BookingID:     bookingID,
					CategoryID:    item.CategoryID,
					NIK:           a.NIK,
					PriceSnapshot: price,
				})
				totalAmount += price
			}
		}

		if err := u.bookingRepo.Create(ctx, tx, booking, items); err != nil {
			return dto.CreateBookingResponse{}, apperror.Internal(err)
		}

		// Publish events setelah commit (best-effort, tidak rollback booking)
		go u.publishPostBooking(context.WithoutCancel(ctx), booking, items, holdItems, totalAmount)

		return dto.CreateBookingResponse{
			BookingID:        bookingID,
			Status:           string(entity.BookingStatusHeld),
			Total:            totalAmount,
			PaymentExpiresAt: paymentExpiresAt.UTC().Format(time.RFC3339),
		}, nil
	})
}

// GetBooking mengembalikan status + sisa TTL + backDisabled.
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
// Initial snapshot dikirim terlebih dahulu sebelum subscribe, sehingga client
// tidak kehilangan state awal saat koneksi baru.
func (u *BookingUsecase) StreamStock(ctx context.Context, eventID string, categoryIDs []string) (<-chan dto.StockEvent, error) {
	// Ambil snapshot awal dari DB — baca sebelum subscribe agar tidak ada gap.
	snapshot, err := u.stockRepo.GetAvailableByEvent(ctx, eventID)
	if err != nil {
		return nil, apperror.Internal(err)
	}

	msgCh, unsub, err := u.cache.SubscribeStockChanged(ctx, eventID)
	if err != nil {
		return nil, apperror.Internal(err)
	}

	out := make(chan dto.StockEvent, 32)

	go func() {
		defer unsub()
		defer close(out)

		// 1. Kirim initial snapshot ke client.
		allSoldOut := len(snapshot) > 0
		for _, cs := range snapshot {
			if cs.Available > 0 {
				allSoldOut = false
			}
			ev := dto.StockEvent{
				EventID:    eventID,
				CategoryID: cs.CategoryID,
				Name:       cs.Name,
				Available:  cs.Available,
			}
			if len(categoryIDs) > 0 && !containsString(categoryIDs, cs.CategoryID) {
				continue
			}
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
		// Bila semua kategori sudah 0 saat connect, kirim sold-out frame.
		if allSoldOut && len(snapshot) > 0 {
			select {
			case out <- dto.StockEvent{EventID: eventID, SoldOut: true}:
			case <-ctx.Done():
				return
			}
		}

		// 2. Subscribe ke Redis Pub/Sub untuk update selanjutnya.
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				var ev dto.StockEvent
				if err := json.Unmarshal(msg, &ev); err != nil {
					u.log.WarnContext(ctx, "stock event unmarshal error", slog.Any("err", err))
					continue
				}
				if len(categoryIDs) > 0 && !ev.SoldOut && !containsString(categoryIDs, ev.CategoryID) {
					continue
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

// ---- Post-booking side effects -----------------------------------------------

func (u *BookingUsecase) publishPostBooking(
	ctx context.Context,
	b entity.Booking,
	items []entity.BookingItem,
	holdItems []entity.HoldItem,
	total int64,
) {
	// 1. booking.created → Notification Worker (email #1 instruksi bayar)
	itemEvents := make([]kafka.BookingItemEvent, 0, len(items))
	for _, it := range items {
		itemEvents = append(itemEvents, kafka.BookingItemEvent{
			CategoryID: it.CategoryID,
			NIK:        it.NIK,
			Price:      it.PriceSnapshot,
		})
	}

	// PaymentExpiresAt dikirim dalam RFC3339 agar worker dapat render deadline bayar yang akurat.
	var paymentExpiresAt string
	if b.PaymentExpiresAt != nil {
		paymentExpiresAt = b.PaymentExpiresAt.UTC().Format(time.RFC3339)
	}

	if err := u.producer.PublishBookingCreated(ctx, b.EventID, kafka.BookingCreatedEvent{
		BookingID:        b.ID,
		EventID:          b.EventID,
		Email:            b.Email,
		Items:            itemEvents,
		TotalAmount:      total,
		PaymentExpiresAt: paymentExpiresAt,
	}); err != nil {
		u.log.ErrorContext(ctx, "publish booking.created failed", slog.Any("err", err))
	}

	// 2. stock.changed (reason=HOLD) → Queue Service update cache stok + SSE broadcast
	for _, h := range holdItems {
		if err := u.producer.PublishStockChanged(ctx, b.EventID, kafka.StockChangedEvent{
			EventID:    b.EventID,
			CategoryID: h.CategoryID,
			Delta:      -h.Qty, // negatif = hold
			Available:  -1,     // -1 = "tidak tahu nilai sekarang"; Queue hitung dari DB bila perlu
			Reason:     "HOLD",
		}); err != nil {
			u.log.ErrorContext(ctx, "publish stock.changed failed", slog.Any("err", err))
		}
	}
}

// ---- Helpers -----------------------------------------------------------------

func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
