// Package outbound mendefinisikan semua interface ke lapis infrastruktur (port outbound hexagon).
package outbound

import (
	"context"
	"time"

	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/dto"
	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/entity"
)

// Tx adalah abstraksi transaksi DB.
// Implementasinya adalah pgx.Tx di infra/db; di test bisa pakai mock.
type Tx interface{}

// IBookingRepository mengelola persistensi booking dan booking_item.
type IBookingRepository interface {
	// CreateBookingTx menjalankan fn dalam satu transaksi pgx.
	// Pool.Begin, defer Rollback, dan Commit dikelola di sini.
	// Usecase tidak perlu import pgxpool.
	CreateBookingTx(
		ctx context.Context,
		req dto.CreateBookingRequest,
		fn func(context.Context, Tx) (dto.CreateBookingResponse, error),
	) (dto.CreateBookingResponse, error)

	// Create menyimpan booking + booking_items dalam transaksi yang sudah berjalan.
	Create(ctx context.Context, tx Tx, booking entity.Booking, items []entity.BookingItem) error

	// GetByID mengembalikan booking beserta items-nya.
	GetByID(ctx context.Context, bookingID string) (entity.Booking, []entity.BookingItem, error)

	// UpdateStatus memperbarui status booking.
	UpdateStatus(ctx context.Context, tx Tx, bookingID string, status entity.BookingStatus) error

	// ListExpiredHeld mengembalikan booking HELD yang payment_expires_at sudah lewat (untuk sweeper).
	ListExpiredHeld(ctx context.Context, limit int) ([]entity.Booking, error)

	// HasActiveHeld mengembalikan true bila masih ada booking HELD aktif untuk event ini.
	// Dipakai sweeper untuk deteksi sold-out: hanya mark sold-out bila stok=0 DAN tidak ada HELD.
	HasActiveHeld(ctx context.Context, eventID string) (bool, error)

	// ExpireBookingTx mengeksekusi UPDATE booking=EXPIRED + ReleaseStock dalam satu transaksi.
	// Transaksi dikelola di sini agar usecase tidak perlu pool.Begin langsung (dan bisa ditest tanpa DB).
	// Mengembalikan items yang di-release untuk Kafka payload.
	ExpireBookingTx(ctx context.Context, bookingID string) ([]entity.BookingItem, error)

	// CancelBookingTx membatalkan booking HELD milik user: lock baris booking (FOR UPDATE),
	// tolak bila PAID (BookingPaid), EXPIRED (BookingExpired) atau sudah ada payment
	// PENDING/SUCCESS (PaymentInProgress), lalu UPDATE status=CANCELLED + ReleaseStock
	// dalam satu transaksi. Sudah CANCELLED → AlreadyCancelled=true tanpa efek (idempoten).
	CancelBookingTx(ctx context.Context, bookingID string) (entity.CancelResult, error)
}

// IStockRepository mengelola perubahan available di ticket_category.
type IStockRepository interface {
	// HoldStock mengurangi available atomic — kondisi WHERE available >= qty.
	// Return StockUnavailable bila 0 row terpengaruh.
	HoldStock(ctx context.Context, tx Tx, items []entity.HoldItem) error

	// ReleaseStock mengembalikan hold (available += qty per item) dalam transaksi.
	ReleaseStock(ctx context.Context, tx Tx, bookingID string) error

	// GetAvailableByEvent mengembalikan snapshot stok per kategori.
	GetAvailableByEvent(ctx context.Context, eventID string) ([]entity.CategoryStock, error)

	// GetPriceMap mengembalikan map[categoryID]price untuk price_snapshot.
	GetPriceMap(ctx context.Context, eventID string) (map[string]int64, error)

	// GetEventName mengembalikan nama event untuk Kafka payload email.
	GetEventName(ctx context.Context, eventID string) (string, error)

	// CountNIKBookings menghitung tiket HELD/PAID per (eventID, nik) di bawah
	// advisory xact lock per NIK, sehingga pengecekan kuota bersifat atomik.
	CountNIKBookings(ctx context.Context, tx Tx, eventID, nik string) (int, error)
}

// IDukcapilClient memanggil Dukcapil mock service.
type IDukcapilClient interface {
	Verify(ctx context.Context, nik, nama, tglLahir, namaIbu string) (bool, error)
}

// ICacheRepository mengelola state di Valkey.
type ICacheRepository interface {
	ValidateAccessToken(ctx context.Context, tokenStr string) (*AccessTokenClaims, error)
	PublishStockChanged(ctx context.Context, eventID string, payload []byte) error
	SubscribeStockChanged(ctx context.Context, eventID string) (<-chan []byte, func(), error)

	// StoreBookingQueueToken menyimpan queueToken milik booking (best-effort) agar cancel
	// bisa melepas kursi antrian walau Access Token sudah kedaluwarsa.
	StoreBookingQueueToken(ctx context.Context, bookingID, queueToken string, ttl time.Duration) error
	// GetBookingQueueToken mengembalikan queueToken booking ("" bila tidak ada).
	GetBookingQueueToken(ctx context.Context, bookingID string) (string, error)
	// RevokeQueueToken menandai queueToken tidak boleh dipakai membuat booking lagi.
	RevokeQueueToken(ctx context.Context, queueToken string, ttl time.Duration) error
	// IsQueueTokenRevoked true bila queueToken sudah dicabut.
	IsQueueTokenRevoked(ctx context.Context, queueToken string) (bool, error)
}

// IEventServiceClient memanggil event-service via HTTP internal API.
type IEventServiceClient interface {
	// MarkSoldOut memanggil POST /internal/events/{eventId}/sold-out ke event-service.
	// Idempoten: event-service mengabaikan bila sudah sold-out.
	MarkSoldOut(ctx context.Context, eventID string) error
}

// IPaymentTokenSigner menerbitkan Payment Token JWT yang dipakai payment-service.
type IPaymentTokenSigner interface {
	// Sign menerbitkan JWT HS256 dengan bookingId + eventId sebagai claims.
	// exp = paymentExpiresAt (UNIX timestamp).
	Sign(bookingID, eventID string, paymentExpiresAt int64) (string, error)
}

// IEventProducer mempublish event ke Kafka.
type IEventProducer interface {
	PublishBookingCreated(ctx context.Context, eventID string, payload any) error
	PublishStockChanged(ctx context.Context, eventID string, payload any) error
	PublishSlotReleased(ctx context.Context, eventID string, payload any) error
	PublishEventSoldOut(ctx context.Context, eventID string, payload any) error
}

// AccessTokenClaims adalah hasil parse Access Token dari Queue Service.
type AccessTokenClaims struct {
	EventID             string
	QueueToken          string
	NIK                 string
	BookingTTLExpiresAt int64
}
