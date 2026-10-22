// Package outbound mendefinisikan semua interface ke lapis infrastruktur (port outbound hexagon).
package outbound

import (
	"context"

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

	// CountNIKBookings menghitung tiket HELD/PAID per (eventID, nik) — SELECT FOR UPDATE.
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
