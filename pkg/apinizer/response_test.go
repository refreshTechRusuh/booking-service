package apinizer_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/refreshTechRusuh/booking-service/pkg/apinizer"
	"github.com/refreshTechRusuh/booking-service/pkg/apperror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func decode(t *testing.T, rr *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var v map[string]any
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&v))
	return v
}

// ---- Success ----------------------------------------------------------------

func TestSuccess_200(t *testing.T) {
	rr := httptest.NewRecorder()
	apinizer.Success(rr, "OK", map[string]string{"id": "1"})
	assert.Equal(t, http.StatusOK, rr.Code)
	resp := decode(t, rr)
	assert.Equal(t, "00000", resp["status"])
	assert.Equal(t, "OK", resp["message"])
	data := resp["data"].(map[string]any)
	assert.Equal(t, "1", data["id"])
}


func TestCreated_200(t *testing.T) {
	rr := httptest.NewRecorder()
	apinizer.Created(rr, "dibuat", struct{ ID string }{ID: "abc"})
	assert.Equal(t, http.StatusOK, rr.Code)
	resp := decode(t, rr)
	assert.Equal(t, "00000", resp["status"])
}

func TestSuccessPartial(t *testing.T) {
	rr := httptest.NewRecorder()
	apinizer.SuccessPartial(rr, "partial", nil)
	assert.Equal(t, http.StatusOK, rr.Code)
	resp := decode(t, rr)
	assert.Equal(t, "00000", resp["status"])
	assert.Equal(t, true, resp["partial"])
}

// ---- WriteError -- AppError mapping ----------------------------------------

func TestWriteError_400_Validation(t *testing.T) {
	rr := httptest.NewRecorder()
	apinizer.WriteError(rr, apperror.Validation("field required"))
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Equal(t, "400", decode(t, rr)["status"])
}

func TestWriteError_400_NIKInvalid(t *testing.T) {
	rr := httptest.NewRecorder()
	apinizer.WriteError(rr, apperror.NIKInvalid("1234"))
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestWriteError_401_TokenInvalid(t *testing.T) {
	rr := httptest.NewRecorder()
	apinizer.WriteError(rr, apperror.TokenInvalid())
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestWriteError_403_Forbidden(t *testing.T) {
	rr := httptest.NewRecorder()
	apinizer.WriteError(rr, apperror.New(apperror.ErrForbidden, "forbidden"))
	assert.Equal(t, http.StatusForbidden, rr.Code)
}

func TestWriteError_404_NotFound(t *testing.T) {
	rr := httptest.NewRecorder()
	apinizer.WriteError(rr, apperror.NotFound("event"))
	assert.Equal(t, http.StatusNotFound, rr.Code)
	assert.Equal(t, "404", decode(t, rr)["status"])
}

func TestWriteError_409_StockUnavailable(t *testing.T) {
	rr := httptest.NewRecorder()
	apinizer.WriteError(rr, apperror.StockUnavailable())
	assert.Equal(t, http.StatusConflict, rr.Code)
}

func TestWriteError_409_QuotaExceeded(t *testing.T) {
	rr := httptest.NewRecorder()
	apinizer.WriteError(rr, apperror.QuotaExceeded())
	assert.Equal(t, http.StatusConflict, rr.Code)
}


func TestWriteError_410_TokenExpired(t *testing.T) {
	rr := httptest.NewRecorder()
	apinizer.WriteError(rr, apperror.TokenExpired())
	assert.Equal(t, http.StatusGone, rr.Code)
	assert.Equal(t, "410", decode(t, rr)["status"])
}

func TestWriteError_410_BookingExpired(t *testing.T) {
	rr := httptest.NewRecorder()
	apinizer.WriteError(rr, apperror.BookingExpired())
	assert.Equal(t, http.StatusGone, rr.Code)
}

func TestWriteError_410_SoldOut(t *testing.T) {
	rr := httptest.NewRecorder()
	apinizer.WriteError(rr, apperror.SoldOut())
	assert.Equal(t, http.StatusGone, rr.Code)
}

func TestWriteError_429_RateLimited(t *testing.T) {
	rr := httptest.NewRecorder()
	apinizer.WriteError(rr, apperror.New(apperror.ErrRateLimited, "too many requests"))
	assert.Equal(t, http.StatusTooManyRequests, rr.Code)
}

func TestWriteError_500_UnhandledError(t *testing.T) {
	rr := httptest.NewRecorder()
	apinizer.WriteError(rr, errors.New("unexpected panic"))
	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	assert.Equal(t, "500", decode(t, rr)["status"])
}

func TestWriteError_500_UnmappedCode(t *testing.T) {
	// Error code yang tidak ada di switch → default 500
	rr := httptest.NewRecorder()
	apinizer.WriteError(rr, apperror.New("UNKNOWN_CODE", "something weird"))
	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}

func TestWriteError_ContentType(t *testing.T) {
	rr := httptest.NewRecorder()
	apinizer.Success(rr, "ok", nil)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))
}

