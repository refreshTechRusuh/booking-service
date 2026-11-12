package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/refreshTechRusuh/booking-service/internal/config"
	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/dto"
	bookinghttp "github.com/refreshTechRusuh/booking-service/internal/booking/infrastructure/http"
	"github.com/refreshTechRusuh/booking-service/pkg/apperror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAccessSecret  = "test-access-secret-booking"
	testPaymentSecret = "test-payment-secret-booking"
)

// ---- Mock usecase ----------------------------------------------------------

type mockBookingUsecase struct {
	createFn      func(ctx context.Context, req dto.CreateBookingRequest) (dto.CreateBookingResponse, error)
	getFn         func(ctx context.Context, bookingID string) (dto.BookingStatusResponse, error)
	streamStockFn func(ctx context.Context, eventID string) (<-chan dto.StockBatchEvent, error)
	cancelFn      func(ctx context.Context, bookingID string) (dto.CancelBookingResponse, error)
}

func (m *mockBookingUsecase) CancelBooking(ctx context.Context, bookingID string) (dto.CancelBookingResponse, error) {
	if m.cancelFn != nil {
		return m.cancelFn(ctx, bookingID)
	}
	return dto.CancelBookingResponse{BookingID: bookingID, Status: "CANCELLED"}, nil
}

func (m *mockBookingUsecase) CreateBooking(ctx context.Context, req dto.CreateBookingRequest) (dto.CreateBookingResponse, error) {
	if m.createFn != nil {
		return m.createFn(ctx, req)
	}
	return dto.CreateBookingResponse{BookingID: "bkg-001", Status: "PENDING", PaymentToken: "tok"}, nil
}

func (m *mockBookingUsecase) GetBooking(ctx context.Context, bookingID string) (dto.BookingStatusResponse, error) {
	if m.getFn != nil {
		return m.getFn(ctx, bookingID)
	}
	return dto.BookingStatusResponse{BookingID: bookingID, Status: "PENDING"}, nil
}

func (m *mockBookingUsecase) StreamStock(ctx context.Context, eventID string) (<-chan dto.StockBatchEvent, error) {
	if m.streamStockFn != nil {
		return m.streamStockFn(ctx, eventID)
	}
	ch := make(chan dto.StockBatchEvent)
	go func() { close(ch) }()
	return ch, nil
}

// ---- Helpers ----------------------------------------------------------------

func setupBookingRouter(uc *mockBookingUsecase) *http.ServeMux {
	mux := http.NewServeMux()
	h := bookinghttp.NewHandler(uc)
	bookinghttp.RegisterRoutes(mux, h, testAccessSecret, testPaymentSecret)
	return mux
}

// makeAccessToken membuat Access Token JWT seperti yang diterbitkan queue-service.
func makeAccessToken(t *testing.T, bookingID, eventID string, ttlExpiry int64) string {
	t.Helper()
	type accessClaims struct {
		EventID             string `json:"eventId"`
		BookingTTLExpiresAt int64  `json:"bookingTtlExpiresAt"`
		jwt.RegisteredClaims
	}
	claims := accessClaims{
		EventID:             eventID,
		BookingTTLExpiresAt: ttlExpiry,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   bookingID,
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString([]byte(testAccessSecret))
	require.NoError(t, err)
	return signed
}

// makePaymentToken membuat Payment Token JWT seperti yang diterbitkan booking-service.
func makePaymentToken(t *testing.T, bookingID, eventID string) string {
	t.Helper()
	type paymentClaims struct {
		BookingID string `json:"bookingId"`
		EventID   string `json:"eventId"`
		jwt.RegisteredClaims
	}
	claims := paymentClaims{
		BookingID: bookingID, EventID: eventID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(10 * time.Minute)),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString([]byte(testPaymentSecret))
	require.NoError(t, err)
	return signed
}

// ---- POST /api/booking/v1/create -------------------------------------------

func TestCreateBooking_Success(t *testing.T) {
	mux := setupBookingRouter(&mockBookingUsecase{})
	ttl := time.Now().Add(5 * time.Minute).Unix()
	token := makeAccessToken(t, "bkg-001", "evt-001", ttl)

	body, _ := json.Marshal(dto.CreateBookingRequest{
		EventID: "evt-001",
		Email:   "user@test.com",
		Items: []dto.BookingItem{
			{CategoryID: "cat-1", Attendees: []dto.AttendeeInput{
				{NIK: "1234567890123456", Nama: "Budi Santoso", TglLahir: "1990-01-01", NamaIbu: "Siti"},
			}},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/booking/v1/create", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Access-Token", token)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, "00000", resp["status"])
}

func TestCreateBooking_NoToken_401(t *testing.T) {
	mux := setupBookingRouter(&mockBookingUsecase{})
	body, _ := json.Marshal(dto.CreateBookingRequest{EventID: "evt-001", Email: "u@test.com"})
	req := httptest.NewRequest(http.MethodPost, "/api/booking/v1/create", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestCreateBooking_InvalidJSON(t *testing.T) {
	mux := setupBookingRouter(&mockBookingUsecase{})
	ttl := time.Now().Add(5 * time.Minute).Unix()
	token := makeAccessToken(t, "bkg-001", "evt-001", ttl)

	req := httptest.NewRequest(http.MethodPost, "/api/booking/v1/create",
		bytes.NewReader([]byte(`{bad json`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Access-Token", token)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}

func TestCreateBooking_UsecaseError(t *testing.T) {
	uc := &mockBookingUsecase{
		createFn: func(_ context.Context, _ dto.CreateBookingRequest) (dto.CreateBookingResponse, error) {
			return dto.CreateBookingResponse{}, apperror.New(apperror.ErrStockUnavailable, "stok habis")
		},
	}
	mux := setupBookingRouter(uc)
	ttl := time.Now().Add(5 * time.Minute).Unix()
	token := makeAccessToken(t, "bkg-001", "evt-001", ttl)
	body, _ := json.Marshal(dto.CreateBookingRequest{
		EventID: "evt-001", Email: "u@test.com",
		Items: []dto.BookingItem{
			{CategoryID: "cat-1", Attendees: []dto.AttendeeInput{
				{NIK: "1234567890123456", Nama: "A", TglLahir: "1990-01-01", NamaIbu: "B"},
			}},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/booking/v1/create", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Access-Token", token)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusConflict, rr.Code)
}

// ---- GET /api/booking/v1/{bookingId} ---------------------------------------

func TestGetBooking_Success(t *testing.T) {
	mux := setupBookingRouter(&mockBookingUsecase{})
	token := makePaymentToken(t, "bkg-001", "evt-001")
	req := httptest.NewRequest(http.MethodGet, "/api/booking/v1/bkg-001", nil)
	req.Header.Set("X-Payment-Token", token)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestGetBooking_NoToken_401(t *testing.T) {
	mux := setupBookingRouter(&mockBookingUsecase{})
	req := httptest.NewRequest(http.MethodGet, "/api/booking/v1/bkg-001", nil)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestGetBooking_NotFound(t *testing.T) {
	uc := &mockBookingUsecase{
		getFn: func(_ context.Context, _ string) (dto.BookingStatusResponse, error) {
			return dto.BookingStatusResponse{}, apperror.New(apperror.ErrNotFound, "booking tidak ditemukan")
		},
	}
	mux := setupBookingRouter(uc)
	token := makePaymentToken(t, "bkg-404", "evt-001")
	req := httptest.NewRequest(http.MethodGet, "/api/booking/v1/bkg-404", nil)
	req.Header.Set("X-Payment-Token", token)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

// ---- GET /api/booking/v1/event/{eventId}/stock/stream ----------------------

func TestStreamStock_NoToken_401(t *testing.T) {
	mux := setupBookingRouter(&mockBookingUsecase{})
	req := httptest.NewRequest(http.MethodGet, "/api/booking/v1/event/evt-001/stock/stream", nil)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestStreamStock_UsecaseError(t *testing.T) {
	uc := &mockBookingUsecase{
		streamStockFn: func(_ context.Context, _ string) (<-chan dto.StockBatchEvent, error) {
			return nil, apperror.New(apperror.ErrNotFound, "event tidak ditemukan")
		},
	}
	mux := setupBookingRouter(uc)
	ttl := time.Now().Add(5 * time.Minute).Unix()
	token := makeAccessToken(t, "bkg-001", "evt-001", ttl)
	req := httptest.NewRequest(http.MethodGet, "/api/booking/v1/event/evt-001/stock/stream", nil)
	req.Header.Set("X-Access-Token", token)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}

// ── Additional GetBooking tests ───────────────────────────────────────────────

func TestGetBooking_InternalError(t *testing.T) {
	uc := &mockBookingUsecase{
		getFn: func(_ context.Context, _ string) (dto.BookingStatusResponse, error) {
			return dto.BookingStatusResponse{}, apperror.New(apperror.ErrInternal, "db error")
		},
	}
	mux := setupBookingRouter(uc)
	token := makePaymentToken(t, "bkg-err", "evt-001")
	req := httptest.NewRequest(http.MethodGet, "/api/booking/v1/bkg-err", nil)
	req.Header.Set("X-Payment-Token", token)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}

func TestGetBooking_Success_BookingIDInResponse(t *testing.T) {
	uc := &mockBookingUsecase{
		getFn: func(_ context.Context, id string) (dto.BookingStatusResponse, error) {
			return dto.BookingStatusResponse{BookingID: id, Status: "HELD", RemainingSeconds: 300}, nil
		},
	}
	mux := setupBookingRouter(uc)
	token := makePaymentToken(t, "bkg-123", "evt-001")
	req := httptest.NewRequest(http.MethodGet, "/api/booking/v1/bkg-123", nil)
	req.Header.Set("X-Payment-Token", token)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	data, ok := resp["data"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "bkg-123", data["bookingId"])
}

// ── Additional CreateBooking tests ────────────────────────────────────────────

func TestCreateBooking_InvalidJSON_Returns500(t *testing.T) {
	mux := setupBookingRouter(&mockBookingUsecase{})
	ttl := time.Now().Add(5 * time.Minute).Unix()
	token := makeAccessToken(t, "bkg-001", "evt-001", ttl)
	req := httptest.NewRequest(http.MethodPost, "/api/booking/v1/create",
		bytes.NewReader([]byte(`{"invalid`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Access-Token", token)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	assert.NotEqual(t, http.StatusOK, rr.Code)
}

// ── NewWiredHandlerWithUC — nil deps ─────────────────────────────────────────

func TestNewWiredHandlerWithUC_nilDeps(t *testing.T) {
	// Konstruktor tidak connect saat dibuat — nil pool/kafka/valkey aman.
	// cfg harus valid karena dipakai untuk JWT secrets.
	cfg := &config.Config{}
	cfg.JWT.AccessTokenSecret = "test-secret"
	cfg.JWT.PaymentTokenSecret = "test-payment"
	cfg.Dukcapil.BaseURL = "http://localhost:8085"
	cfg.Dukcapil.TimeoutMs = 100
	cfg.EventSvc.BaseURL = "http://localhost:8082"

	h, uc := bookinghttp.NewWiredHandlerWithUC(context.Background(), cfg, nil, nil, nil)
	assert.NotNil(t, h)
	assert.NotNil(t, uc)
}

// ---- POST /api/booking/v1/{bookingId}/cancel --------------------------------

func doCancel(mux *http.ServeMux, bookingID, paymentToken string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/booking/v1/"+bookingID+"/cancel", nil)
	if paymentToken != "" {
		req.Header.Set("X-Payment-Token", paymentToken)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func TestCancelBooking_ShouldReturn200_WhenPaymentTokenMatches(t *testing.T) {
	mux := setupBookingRouter(&mockBookingUsecase{})
	rec := doCancel(mux, "bkg-1", makePaymentToken(t, "bkg-1", "evt-1"))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CANCELLED")
}

func TestCancelBooking_ShouldReturn401_WhenNoPaymentToken(t *testing.T) {
	mux := setupBookingRouter(&mockBookingUsecase{})
	assert.Equal(t, http.StatusUnauthorized, doCancel(mux, "bkg-1", "").Code)
}

func TestCancelBooking_ShouldReturn403_WhenTokenBelongsToOtherBooking(t *testing.T) {
	called := false
	mux := setupBookingRouter(&mockBookingUsecase{cancelFn: func(context.Context, string) (dto.CancelBookingResponse, error) {
		called = true
		return dto.CancelBookingResponse{}, nil
	}})
	rec := doCancel(mux, "bkg-2", makePaymentToken(t, "bkg-1", "evt-1"))
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.False(t, called, "usecase tidak boleh dipanggil")
}

func TestCancelBooking_ShouldReturn409_WhenPaymentInProgress(t *testing.T) {
	mux := setupBookingRouter(&mockBookingUsecase{cancelFn: func(context.Context, string) (dto.CancelBookingResponse, error) {
		return dto.CancelBookingResponse{}, apperror.PaymentInProgress()
	}})
	rec := doCancel(mux, "bkg-1", makePaymentToken(t, "bkg-1", "evt-1"))
	assert.Equal(t, http.StatusConflict, rec.Code)
}
