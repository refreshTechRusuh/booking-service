// Package entity mendefinisikan domain entity booking-service.
package entity

import "time"

// BookingStatus adalah status state machine booking.
type BookingStatus string

const (
	BookingStatusHeld      BookingStatus = "HELD"
	BookingStatusPaid      BookingStatus = "PAID"
	BookingStatusExpired   BookingStatus = "EXPIRED"
	BookingStatusCancelled BookingStatus = "CANCELLED"
)
// Booking adalah agregat utama — satu transaksi booking tiket.
type Booking struct {
	ID               string
	UserID           string        // nullable: kosong untuk guest
	EventID          string
	Email            string        // wajib — tujuan e-tiket
	Status           BookingStatus
	BookingExpiresAt *time.Time    // dari Access Token TTL (5 menit)
	PaymentExpiresAt *time.Time    // dari saat booking dibuat (10 menit)
	CreatedAt        time.Time
}

// BookingItem adalah satu tiket dalam booking (1 tiket = 1 NIK penonton).
type BookingItem struct {
	ID            string
	BookingID     string
	CategoryID    string
	NIK           string // NIK penonton — diverifikasi Dukcapil
	PriceSnapshot int64  // harga saat booking (immutable)
}

// CancelResult adalah hasil CancelBookingTx.
type CancelResult struct {
	EventID          string
	Items            []BookingItem // item yang stoknya dilepas
	AlreadyCancelled bool          // true bila booking sudah CANCELLED sebelumnya (idempoten, tanpa efek)
}

// HoldItem adalah input untuk HoldStock.
type HoldItem struct {
	CategoryID string
	Qty        int
}

// CategoryStock adalah snapshot stok per kategori.
type CategoryStock struct {
	CategoryID string
	Name       string
	Available  int
}

// AttendeeInput adalah input satu penonton dari request body.
type AttendeeInput struct {
	NIK      string
	Nama     string
	TglLahir string // format YYYY-MM-DD
	NamaIbu  string
}
