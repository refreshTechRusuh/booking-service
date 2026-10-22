// Package outbound mendefinisikan semua interface ke lapis infrastruktur (port outbound hexagon).
package outbound

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/entity"
)

// IBookingRepository mengelola persistensi booking dan booking_item.
type IBookingRepository interface {
	// Create menyimpan booking + booking_item dalam transaksi yang sudah berjalan.
	Create(ctx context.Context, tx pgx.Tx, booking entity.Booking, items []entity.BookingItem) error

	// GetByID mengembalikan booking beserta items-nya.
	GetByID(ctx context.Context, bookingID string) (entity.Booking, []entity.BookingItem, error)

	// UpdateStatus memperbarui status booking (HELD→PAID/EXPIRED/CANCELLED).
	UpdateStatus(ctx context.Context, tx pgx.Tx, bookingID string, status entity.BookingStatus) error

	// ListExpiredHeld mengembalikan booking HELD yang payment_expires_at sudah lewat (untuk sweeper).
	ListExpiredHeld(ctx context.Context, limit int) ([]entity.Booking, error)
}

// IStockRepository mengelola perubahan available di ticket_category.
type IStockRepository interface {
	// HoldStock mengurangi available atomic dalam transaksi yang sudah berjalan.
	// Mengembalikan error StockUnavailable jika 0 row terpengaruh.
	HoldStock(ctx context.Context, tx pgx.Tx, items []entity.HoldItem) error

	// ReleaseStock mengembalikan hold (available += qty) dalam transaksi.
	ReleaseStock(ctx context.Context, tx pgx.Tx, bookingID string) error

	// GetAvailableByEvent mengembalikan semua kategori dan available untuk suatu event.
	GetAvailableByEvent(ctx context.Context, eventID string) ([]entity.CategoryStock, error)

	// CountNIKBookings menghitung tiket aktif/terbayar per (eventID, nik) dengan SELECT FOR UPDATE.
	CountNIKBookings(ctx context.Context, tx pgx.Tx, eventID, nik string) (int, error)
}

// IDukcapilClient memanggil Dukcapil mock service.
type IDukcapilClient interface {
	// Verify mengembalikan true jika keempat field cocok dengan record Dukcapil.
	// Jika service down dan failOpen=true → return true + log warning.
	Verify(ctx context.Context, nik, nama, tglLahir, namaIbu string) (bool, error)
}

// ICacheRepository mengelola state di Valkey (Access Token, Pub/Sub stok).
type ICacheRepository interface {
	// ValidateAccessToken memverifikasi JWT dan mengembalikan claims-nya.
	ValidateAccessToken(ctx context.Context, tokenStr string) (*AccessTokenClaims, error)

	// PublishStockChanged mempublish pesan ke channel Redis Pub/Sub untuk SSE broadcast.
	PublishStockChanged(ctx context.Context, eventID string, payload []byte) error

	// SubscribeStockChanged subscribe ke channel Redis Pub/Sub.
	// Mengembalikan channel pesan dan unsubscribe func.
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
	NIK                 string // kosong untuk guest
	BookingTTLExpiresAt int64  // unix timestamp
}
