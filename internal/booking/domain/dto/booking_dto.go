// Package dto mendefinisikan Data Transfer Object untuk booking-service.
package dto

import "github.com/refreshTechRusuh/booking-service/internal/booking/domain/entity"

// ---- Request ---------------------------------------------------------------

// CreateBookingRequest adalah body POST /bookings.
type CreateBookingRequest struct {
	EventID string        `json:"eventId"  validate:"required"`
	Email   string        `json:"email"    validate:"required,email"`
	Items   []BookingItem `json:"items"    validate:"required,min=1"`

	// Di-set middleware/handler, bukan dari body.
	UserID              string
	AccessToken         string
	EventIDFromToken    string
	BookingTTLExpiresAt int64
	QueueToken          string // claim queueToken dari Access Token
}

// BookingItem adalah satu kategori + daftar penonton.
type BookingItem struct {
	CategoryID string          `json:"categoryId" validate:"required"`
	Attendees  []AttendeeInput `json:"attendees"  validate:"required,min=1"`
}

// AttendeeInput adalah satu penonton — 4 field diverifikasi ke Dukcapil.
type AttendeeInput struct {
	NIK      string `json:"nik"      validate:"required,len=16"`
	Nama     string `json:"nama"     validate:"required"`
	TglLahir string `json:"tglLahir" validate:"required"`
	NamaIbu  string `json:"namaIbu"  validate:"required"`
}

// ---- Response --------------------------------------------------------------

// CreateBookingResponse adalah response 200 POST /bookings.
type CreateBookingResponse struct {
	BookingID        string `json:"bookingId"`
	Status           string `json:"status"`
	Total            int64  `json:"total"`
	PaymentExpiresAt string `json:"paymentExpiresAt"`
	PaymentToken     string `json:"paymentToken"`
}

// BookingStatusResponse adalah response GET /bookings/{bookingId}.
type BookingStatusResponse struct {
	BookingID        string `json:"bookingId"`
	Status           string `json:"status"`
	RemainingSeconds int    `json:"remainingSeconds"`
	BackDisabled     bool   `json:"backDisabled"`
}

// CancelBookingResponse adalah response POST /bookings/{bookingId}/cancel.
type CancelBookingResponse struct {
	BookingID string `json:"bookingId"`
	Status    string `json:"status"` // CANCELLED
}

// StockEvent adalah satu frame SSE dari GET /events/{eventId}/stock/stream.
type StockEvent struct {
	EventID    string `json:"eventId"`
	CategoryID string `json:"categoryId"`
	Name       string `json:"name,omitempty"` // nama kategori (VIP/Festival/...), selalu diisi di initial snapshot
	Available  int    `json:"available"`
	SoldOut    bool   `json:"soldOut,omitempty"` // true bila event ini sold-out
}

// StockSnapshot adalah initial snapshot seluruh kategori saat SSE connect.
type StockSnapshot struct {
	EventID    string       `json:"eventId"`
	Categories []StockEvent `json:"categories"`
}

// StockBatchEvent adalah frame SSE batch — semua kategori per event sekaligus.
// Dikirim setiap 1 detik bila ada perubahan, atau langsung saat initial snapshot.
// Menggantikan StockEvent per-frame untuk mengurangi flooding ke client.
type StockBatchEvent struct {
	EventID    string       `json:"eventId"`
	Categories []StockEvent `json:"categories,omitempty"`
	SoldOut    bool         `json:"soldOut,omitempty"` // true bila event ini sold-out total
}

// ---- Helpers ---------------------------------------------------------------

// ToHoldItems mengkonversi items request ke []entity.HoldItem.
func (req CreateBookingRequest) ToHoldItems() []entity.HoldItem {
	m := make(map[string]int)
	for _, item := range req.Items {
		m[item.CategoryID] += len(item.Attendees)
	}
	result := make([]entity.HoldItem, 0, len(m))
	for catID, qty := range m {
		result = append(result, entity.HoldItem{CategoryID: catID, Qty: qty})
	}
	return result
}

// TotalTickets mengembalikan total tiket dalam request (maks 3).
func (req CreateBookingRequest) TotalTickets() int {
	total := 0
	for _, item := range req.Items {
		total += len(item.Attendees)
	}
	return total
}

// NIKQtyMap mengembalikan map[nik]qty untuk validasi kuota per-NIK.
func (req CreateBookingRequest) NIKQtyMap() map[string]int {
	m := make(map[string]int)
	for _, item := range req.Items {
		for _, a := range item.Attendees {
			m[a.NIK]++
		}
	}
	return m
}

// PriceSnapshot adalah placeholder — inject harga dari event-service di handler.
func (req CreateBookingRequest) PriceSnapshot(_ string) int64 {
	return 0 // TODO: inject dari request extension atau internal call ke event-service
}
