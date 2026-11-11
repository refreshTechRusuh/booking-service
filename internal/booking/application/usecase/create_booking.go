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
	maxTicketsPerNIK      = 1 // 1 NIK hanya boleh 1 tiket per event (BR-01)
	paymentTTLMinutes     = 10
	backDisabledThreshold = 30 // seconds
)

// BookingUsecase mengimplementasikan inbound.IBookingUsecase.
type BookingUsecase struct {
	pool         *pgxpool.Pool
	bookingRepo  outbound.IBookingRepository
	stockRepo    outbound.IStockRepository
	dukcapil     outbound.IDukcapilClient
	cache        outbound.ICacheRepository
	producer     outbound.IEventProducer
	eventClient  outbound.IEventServiceClient
	paymentToken outbound.IPaymentTokenSigner
	log          *slog.Logger
}

func NewBookingUsecase(
	pool *pgxpool.Pool,
	bookingRepo outbound.IBookingRepository,
	stockRepo outbound.IStockRepository,
	dukcapil outbound.IDukcapilClient,
	cache outbound.ICacheRepository,
	producer outbound.IEventProducer,
	eventClient outbound.IEventServiceClient,
	paymentToken outbound.IPaymentTokenSigner,
	log *slog.Logger,
) *BookingUsecase {
	return &BookingUsecase{
		pool: pool, bookingRepo: bookingRepo, stockRepo: stockRepo,
		dukcapil: dukcapil, cache: cache, producer: producer,
		eventClient: eventClient, paymentToken: paymentToken, log: log,
	}
}

// CreateBooking mengeksekusi 6 langkah validasi + hold stok dalam satu transaksi pgx.
func (u *BookingUsecase) CreateBooking(ctx context.Context, req dto.CreateBookingRequest) (dto.CreateBookingResponse, error) {
	// Validasi: eventId dari Access Token harus cocok dengan eventId di body.
	if req.EventIDFromToken != "" && req.EventIDFromToken != req.EventID {
		return dto.CreateBookingResponse{}, apperror.TokenInvalid()
	}

	// Validasi: booking TTL dari Queue Service belum expired.
	// BookingTTLExpiresAt = 0 berarti tidak ada TTL (token lokal/dev tanpa TTL), lewatkan.
	if req.BookingTTLExpiresAt > 0 && time.Now().Unix() > req.BookingTTLExpiresAt {
		return dto.CreateBookingResponse{}, apperror.TokenExpired()
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

		// Terbitkan Payment Token — exp = paymentExpiresAt agar berlaku selama TTL bayar (10 mnt).
		paymentToken, err := u.paymentToken.Sign(bookingID, req.EventID, paymentExpiresAt.Unix())
		if err != nil {
			u.log.ErrorContext(ctx, "sign payment token failed", slog.Any("err", err))
			return dto.CreateBookingResponse{}, apperror.Internal(err)
		}

		// Publish events setelah commit (best-effort, tidak rollback booking).
		// Bangun nikToName dari req sebelum goroutine agar req tidak diakses concurrent.
		nikToName := make(map[string]string, len(items))
		for _, ri := range req.Items {
			for _, a := range ri.Attendees {
				nikToName[a.NIK] = a.Nama
			}
		}
		go u.publishPostBooking(context.WithoutCancel(ctx), booking, items, holdItems, totalAmount, nikToName)

		return dto.CreateBookingResponse{
			BookingID:        bookingID,
			Status:           string(entity.BookingStatusHeld),
			Total:            totalAmount,
			PaymentExpiresAt: paymentExpiresAt.UTC().Format(time.RFC3339),
			PaymentToken:     paymentToken,
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

// StreamStock mengembalikan channel batch event stok dari Redis Pub/Sub.
// Initial snapshot dikirim langsung saat connect. Selanjutnya, pesan Redis
// di-accumulate ke buffer map dan di-flush setiap 1 detik sebagai satu
// StockBatchEvent, sehingga client tidak kena flooding saat banyak booking serentak.
func (u *BookingUsecase) StreamStock(ctx context.Context, eventID string) (<-chan dto.StockBatchEvent, error) {
	// Ambil snapshot awal dari DB — baca sebelum subscribe agar tidak ada gap.
	snapshot, err := u.stockRepo.GetAvailableByEvent(ctx, eventID)
	if err != nil {
		return nil, apperror.Internal(err)
	}

	msgCh, unsub, err := u.cache.SubscribeStockChanged(ctx, eventID)
	if err != nil {
		return nil, apperror.Internal(err)
	}

	out := make(chan dto.StockBatchEvent, 4)

	go func() {
		defer unsub()
		defer close(out)

		// 1. Kirim initial snapshot langsung sebagai satu StockBatchEvent.
		allSoldOut := len(snapshot) > 0
		categories := make([]dto.StockEvent, 0, len(snapshot))
		for _, cs := range snapshot {
			if cs.Available > 0 {
				allSoldOut = false
			}
			categories = append(categories, dto.StockEvent{
				EventID:    eventID,
				CategoryID: cs.CategoryID,
				Name:       cs.Name,
				Available:  cs.Available,
			})
		}

		if allSoldOut && len(snapshot) > 0 {
			select {
			case out <- dto.StockBatchEvent{EventID: eventID, SoldOut: true}:
			case <-ctx.Done():
				return
			}
			return
		}

		if len(categories) > 0 {
			select {
			case out <- dto.StockBatchEvent{EventID: eventID, Categories: categories}:
			case <-ctx.Done():
				return
			}
		}

		// 2. Accumulate Redis messages dan flush setiap 1 detik.
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		buffer := make(map[string]dto.StockEvent) // categoryID → latest StockEvent
		soldOutFlag := false

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
				if ev.SoldOut {
					soldOutFlag = true
				} else {
					buffer[ev.CategoryID] = ev
				}

			case <-ticker.C:
				if soldOutFlag {
					select {
					case out <- dto.StockBatchEvent{EventID: eventID, SoldOut: true}:
					case <-ctx.Done():
					}
					return
				}
				if len(buffer) == 0 {
					// Tidak ada perubahan sejak flush terakhir — tidak kirim frame kosong.
					continue
				}
				vals := make([]dto.StockEvent, 0, len(buffer))
				for _, ev := range buffer {
					vals = append(vals, ev)
				}
				select {
				case out <- dto.StockBatchEvent{EventID: eventID, Categories: vals}:
				case <-ctx.Done():
					return
				}
				// Clear buffer setelah flush.
				buffer = make(map[string]dto.StockEvent)
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
	nikToName map[string]string,
) {
	// Ambil nama event (best-effort, fallback ke kosong kalau gagal)
	eventName, err := u.stockRepo.GetEventName(ctx, b.EventID)
	if err != nil {
		u.log.WarnContext(ctx, "get event name failed", slog.Any("err", err))
	}

	// 1. booking.created → Notification Worker (email #1 instruksi bayar)
	itemEvents := make([]kafka.BookingItemEvent, 0, len(items))
	for _, it := range items {
		itemEvents = append(itemEvents, kafka.BookingItemEvent{
			CategoryID:   it.CategoryID,
			CategoryName: "", // akan diisi dari nameMap setelah GetAvailableByEvent
			NIK:          it.NIK,
			Name:         nikToName[it.NIK],
			Price:        it.PriceSnapshot,
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
		EventName:        eventName,
		Email:            b.Email,
		Items:            itemEvents,
		TotalAmount:      total,
		PaymentExpiresAt: paymentExpiresAt,
	}); err != nil {
		u.log.ErrorContext(ctx, "publish booking.created failed", slog.Any("err", err))
	}

	// 2. Baca snapshot stok dari DB sekali — dipakai bersama oleh Kafka (stock.changed)
	//    dan Redis Pub/Sub (SSE), sehingga kedua consumer menerima nilai aktual yang sama.
	stocks, err := u.stockRepo.GetAvailableByEvent(ctx, b.EventID)
	if err != nil {
		u.log.WarnContext(ctx, "get available after hold failed", slog.Any("err", err))
	}
	stockMap := make(map[string]int, len(stocks))
	nameMap := make(map[string]string, len(stocks))
	for _, s := range stocks {
		stockMap[s.CategoryID] = s.Available
		nameMap[s.CategoryID] = s.Name
	}

	// Isi CategoryName di itemEvents dari nameMap (tersedia setelah GetAvailableByEvent).
	for i := range itemEvents {
		itemEvents[i].CategoryName = nameMap[itemEvents[i].CategoryID]
	}

	// 3. stock.changed (reason=HOLD) → Queue Service update cache stok
	//    Available diisi nilai aktual dari DB agar roomcap queue-service tetap akurat.
	for _, h := range holdItems {
		if err := u.producer.PublishStockChanged(ctx, b.EventID, kafka.StockChangedEvent{
			EventID:    b.EventID,
			CategoryID: h.CategoryID,
			Delta:      -h.Qty,           // negatif = hold
			Available:  stockMap[h.CategoryID], // nilai aktual post-hold dari DB
			Reason:     "HOLD",
		}); err != nil {
			u.log.ErrorContext(ctx, "publish stock.changed failed", slog.Any("err", err))
		}
	}

	// 4. Redis Pub/Sub → SSE: kirim frame stok terbaru per kategori yang di-hold,
	//    supaya klien SSE lain melihat stok TURUN live saat ada booking.
	//    Menggunakan stockMap yang sudah dibaca di langkah 2 (tidak perlu baca DB lagi).
	for _, h := range holdItems {
		ev := dto.StockEvent{
			EventID:    b.EventID,
			CategoryID: h.CategoryID,
			Name:       nameMap[h.CategoryID],
			Available:  stockMap[h.CategoryID], // nilai available aktual setelah hold
		}
		if bz, marshalErr := json.Marshal(ev); marshalErr == nil {
			if pubErr := u.cache.PublishStockChanged(ctx, b.EventID, bz); pubErr != nil {
				u.log.WarnContext(ctx, "redis publish stock.changed (HOLD) failed", slog.Any("err", pubErr))
			}
		}
	}
}

// ---- Helpers -----------------------------------------------------------------
