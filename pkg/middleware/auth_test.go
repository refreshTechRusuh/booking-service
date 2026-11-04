package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/refreshTechRusuh/booking-service/pkg/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAccessSecret  = "test-access-secret-booking"
	testPaymentSecret = "test-payment-secret-booking"
)

var okHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
})

// ---- helpers ----------------------------------------------------------------

func makeAccessJWT(t *testing.T, secret string, ttlExpiry int64, expired bool) string {
	t.Helper()
	exp := time.Now().Add(5 * time.Minute)
	if expired {
		exp = time.Now().Add(-1 * time.Minute)
	}
	claims := jwt.MapClaims{
		"eventId":             "evt-001",
		"bookingTtlExpiresAt": ttlExpiry,
		"exp":                 exp.Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString([]byte(secret))
	require.NoError(t, err)
	return signed
}

func makePaymentJWT(t *testing.T, secret, bookingID, eventID string, expired bool) string {
	t.Helper()
	exp := time.Now().Add(10 * time.Minute)
	if expired {
		exp = time.Now().Add(-1 * time.Minute)
	}
	claims := jwt.MapClaims{
		"bookingId": bookingID,
		"eventId":   eventID,
		"exp":       exp.Unix(),
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString([]byte(secret))
	require.NoError(t, err)
	return signed
}

// ---- RequireAccessToken -----------------------------------------------------

func TestRequireAccessToken_NoHeader_401(t *testing.T) {
	h := middleware.RequireAccessToken(testAccessSecret)(okHandler)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestRequireAccessToken_OpaqueToken_401(t *testing.T) {
	// Booking-service RequireAccessToken TIDAK punya opaque bypass → reject semua non-JWT
	h := middleware.RequireAccessToken(testAccessSecret)(okHandler)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Access-Token", "opaque-token-non-jwt")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestRequireAccessToken_ValidJWT_Pass(t *testing.T) {
	ttl := time.Now().Add(5 * time.Minute).Unix()
	token := makeAccessJWT(t, testAccessSecret, ttl, false)
	h := middleware.RequireAccessToken(testAccessSecret)(okHandler)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Access-Token", token)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestRequireAccessToken_ExpiredJWT_401(t *testing.T) {
	token := makeAccessJWT(t, testAccessSecret, 0, true)
	h := middleware.RequireAccessToken(testAccessSecret)(okHandler)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Access-Token", token)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestRequireAccessToken_ValidJWT_TTL_Ignored(t *testing.T) {
	// Booking-service tidak cek bookingTtlExpiresAt -- hanya cek JWT exp
	ttl := time.Now().Add(-2 * time.Minute).Unix() // TTL expired tapi JWT masih valid
	token := makeAccessJWT(t, testAccessSecret, ttl, false)
	h := middleware.RequireAccessToken(testAccessSecret)(okHandler)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Access-Token", token)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	// JWT masih valid (exp belum lewat) → booking middleware PASS
	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestRequireAccessToken_WrongSecret_401(t *testing.T) {
	token := makeAccessJWT(t, "wrong-secret", 0, false)
	h := middleware.RequireAccessToken(testAccessSecret)(okHandler)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Access-Token", token)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestRequireAccessToken_ClaimsInjected(t *testing.T) {
	ttl := time.Now().Add(5 * time.Minute).Unix()
	token := makeAccessJWT(t, testAccessSecret, ttl, false)

	var gotClaims *middleware.AccessTokenClaims
	checkHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotClaims, _ = middleware.GetAccessTokenClaims(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	h := middleware.RequireAccessToken(testAccessSecret)(checkHandler)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Access-Token", token)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	require.NotNil(t, gotClaims)
	assert.Equal(t, "evt-001", gotClaims.EventID)
}

// ---- RequirePaymentToken ----------------------------------------------------

func TestRequirePaymentToken_NoHeader_401(t *testing.T) {
	h := middleware.RequirePaymentToken(testPaymentSecret)(okHandler)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestRequirePaymentToken_ValidToken_Pass(t *testing.T) {
	token := makePaymentJWT(t, testPaymentSecret, "bkg-001", "evt-001", false)
	h := middleware.RequirePaymentToken(testPaymentSecret)(okHandler)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Payment-Token", token)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestRequirePaymentToken_ExpiredToken_401(t *testing.T) {
	token := makePaymentJWT(t, testPaymentSecret, "bkg-001", "evt-001", true)
	h := middleware.RequirePaymentToken(testPaymentSecret)(okHandler)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Payment-Token", token)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestRequirePaymentToken_WrongSecret_401(t *testing.T) {
	token := makePaymentJWT(t, "wrong-secret", "bkg-001", "evt-001", false)
	h := middleware.RequirePaymentToken(testPaymentSecret)(okHandler)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Payment-Token", token)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestRequirePaymentToken_ClaimsInjected(t *testing.T) {
	token := makePaymentJWT(t, testPaymentSecret, "bkg-001", "evt-001", false)

	var gotClaims *middleware.PaymentTokenClaims
	checkHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotClaims, _ = middleware.GetPaymentTokenClaims(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	h := middleware.RequirePaymentToken(testPaymentSecret)(checkHandler)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-Payment-Token", token)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	require.NotNil(t, gotClaims)
	assert.Equal(t, "bkg-001", gotClaims.BookingID)
	assert.Equal(t, "evt-001", gotClaims.EventID)
}

// ---- Chain ------------------------------------------------------------------

func TestChain_MultipleMiddlewares(t *testing.T) {
	// Chain(handler, m1, m2): m1 outermost → m1 runs first
	order := []string{}
	m1 := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			order = append(order, "m1")
			next.ServeHTTP(w, r)
		})
	}
	m2 := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			order = append(order, "m2")
			next.ServeHTTP(w, r)
		})
	}
	h := middleware.Chain(okHandler, m1, m2)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	assert.Equal(t, []string{"m1", "m2"}, order)
}
