// Package inbound mendefinisikan interface usecase booking (port inbound hexagon).
package inbound

import (
	"context"

	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/dto"
)

// IBookingUsecase adalah kontrak yang dipanggil oleh HTTP handler.
type IBookingUsecase interface {
	// CreateBooking memvalidasi, hold stok atomic, dan membuat booking baru.
	// Input: Access Token sudah diverifikasi di middleware.
	CreateBooking(ctx context.Context, req dto.CreateBookingRequest) (dto.CreateBookingResponse, error)

	// GetBooking mengembalikan status booking + sisa TTL + backDisabled flag.
	GetBooking(ctx context.Context, bookingID string) (dto.BookingStatusResponse, error)

	// CancelBooking membatalkan booking HELD (user keluar total dari booking room):
	// status CANCELLED, stok dilepas, queue token dicabut, event Kafka/SSE dipublish.
	CancelBooking(ctx context.Context, bookingID string) (dto.CancelBookingResponse, error)

	// StreamStock dipanggil oleh SSE handler untuk mendapatkan channel batch update stok.
	// Frame dikirim langsung saat connect (initial snapshot) dan setiap 1 detik bila ada perubahan.
	// Handler menutup done channel saat koneksi SSE terputus.
	StreamStock(ctx context.Context, eventID string) (<-chan dto.StockBatchEvent, error)
}
