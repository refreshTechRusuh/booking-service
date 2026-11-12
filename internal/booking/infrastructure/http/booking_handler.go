// Package http adalah HTTP adapter untuk booking domain (infrastructure layer).
package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/dto"
	"github.com/refreshTechRusuh/booking-service/internal/booking/port/inbound"
	"github.com/refreshTechRusuh/booking-service/pkg/apinizer"
	"github.com/refreshTechRusuh/booking-service/pkg/apperror"
	"github.com/refreshTechRusuh/booking-service/pkg/middleware"
)

var validate = validator.New()

// Handler memegang semua HTTP handler untuk booking domain.
type Handler struct {
	usecase inbound.IBookingUsecase
}

func NewHandler(usecase inbound.IBookingUsecase) *Handler {
	return &Handler{usecase: usecase}
}

// RegisterRoutes mendaftarkan semua route booking ke mux.
// Dipanggil dari delivery/server.go.
func RegisterRoutes(mux *http.ServeMux, h *Handler, accessTokenSecret, paymentTokenSecret string) {
	// Middleware chain untuk route yang butuh Access Token (booking room).
	withAccessToken := func(next http.HandlerFunc) http.Handler {
		return middleware.Chain(
			http.HandlerFunc(next),
			middleware.RequireAccessToken(accessTokenSecret),
		)
	}
	// Middleware chain untuk route yang butuh Payment Token.
	withPaymentToken := func(next http.HandlerFunc) http.Handler {
		return middleware.Chain(
			http.HandlerFunc(next),
			middleware.RequirePaymentToken(paymentTokenSecret),
		)
	}

	mux.Handle("POST /api/booking/v1/create", withAccessToken(h.CreateBooking))
	// GET /{bookingId} dipanggil dari Payment Page — pakai Payment Token agar tetap valid
	// setelah menit ke-5 saat Access Token sudah expired.
	mux.Handle("GET /api/booking/v1/{bookingId}", withPaymentToken(h.GetBooking))
	// POST /{bookingId}/cancel — user keluar total; Payment Token dengan bookingId yang cocok.
	mux.Handle("POST /api/booking/v1/{bookingId}/cancel", withPaymentToken(h.CancelBooking))
	mux.Handle("GET /api/booking/v1/event/{eventId}/stock/stream", withAccessToken(h.StreamStock))
}

// CreateBooking handles POST /api/v1/bookings
func (h *Handler) CreateBooking(w http.ResponseWriter, r *http.Request) {
	var req dto.CreateBookingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apinizer.WriteError(w, fmt.Errorf("invalid JSON body"))
		return
	}
	if err := validate.Struct(req); err != nil {
		apinizer.WriteError(w, fmt.Errorf("validation: %s", err.Error()))
		return
	}

	// Inject claims dari context (di-set oleh RequireAccessToken middleware).
	if claims, ok := middleware.GetAccessTokenClaims(r.Context()); ok {
		req.EventIDFromToken = claims.EventID
		req.BookingTTLExpiresAt = claims.BookingTTLExpiresAt
		req.QueueToken = claims.QueueToken
	}
	if userID, ok := middleware.GetUserID(r.Context()); ok {
		req.UserID = userID
	}

	resp, err := h.usecase.CreateBooking(r.Context(), req)
	if err != nil {
		apinizer.WriteError(w, err)
		return
	}

	apinizer.Success(w, "Booking dibuat", resp)
}

// GetBooking handles GET /api/v1/bookings/{bookingId}
func (h *Handler) GetBooking(w http.ResponseWriter, r *http.Request) {
	bookingID := r.PathValue("bookingId")
	if bookingID == "" {
		apinizer.WriteError(w, fmt.Errorf("bookingId required"))
		return
	}

	// Payment Token harus milik booking ini (bukan booking orang lain).
	if claims, ok := middleware.GetPaymentTokenClaims(r.Context()); !ok || claims.BookingID != bookingID {
		apinizer.WriteError(w, apperror.New(apperror.ErrForbidden, "Payment Token tidak sesuai dengan booking"))
		return
	}

	resp, err := h.usecase.GetBooking(r.Context(), bookingID)
	if err != nil {
		apinizer.WriteError(w, err)
		return
	}

	apinizer.Success(w, "OK", resp)
}

// CancelBooking handles POST /api/booking/v1/{bookingId}/cancel
func (h *Handler) CancelBooking(w http.ResponseWriter, r *http.Request) {
	bookingID := r.PathValue("bookingId")
	if bookingID == "" {
		apinizer.WriteError(w, apperror.Validation("bookingId required"))
		return
	}
	// Payment Token harus milik booking ini (bukan booking orang lain).
	claims, ok := middleware.GetPaymentTokenClaims(r.Context())
	if !ok || claims.BookingID != bookingID {
		apinizer.WriteError(w, apperror.New(apperror.ErrForbidden, "Payment Token tidak sesuai dengan booking"))
		return
	}

	resp, err := h.usecase.CancelBooking(r.Context(), bookingID)
	if err != nil {
		apinizer.WriteError(w, err)
		return
	}
	apinizer.Success(w, "Booking dibatalkan", resp)
}

// StreamStock handles GET /api/booking/v1/event/{eventId}/stock/stream (SSE)
// Format data: StockBatchEvent — semua kategori sekaligus, dikirim setiap 1 detik.
// Initial snapshot dikirim langsung saat koneksi tanpa delay.
func (h *Handler) StreamStock(w http.ResponseWriter, r *http.Request) {
	eventID := r.PathValue("eventId")
	if eventID == "" {
		http.Error(w, "eventId required", http.StatusBadRequest)
		return
	}

	// Set SSE headers.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	stockCh, err := h.usecase.StreamStock(ctx, eventID)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Kirim connected comment agar klien tahu stream siap.
	fmt.Fprintf(w, ": connected to stock stream for event %s\n\n", eventID) // #nosec G705 -- SSE comment, not rendered as HTML
	flusher.Flush()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Heartbeat comment untuk mencegah koneksi timeout.
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		case ev, ok := <-stockCh:
			if !ok {
				return
			}
			b, _ := json.Marshal(ev)
			fmt.Fprintf(w, "event: stock\ndata: %s\n\n", b)
			flusher.Flush()
		}
	}
}
