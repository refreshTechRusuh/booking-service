package apperror_test

import (
	"errors"
	"testing"

	"github.com/refreshTechRusuh/booking-service/pkg/apperror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_ErrorString(t *testing.T) {
	e := apperror.New(apperror.ErrNotFound, "event tidak ditemukan")
	assert.Equal(t, "[NOT_FOUND] event tidak ditemukan", e.Error())
}

func TestWrap_ErrorStringWithCause(t *testing.T) {
	cause := errors.New("db connection refused")
	e := apperror.Wrap(apperror.ErrInternal, "server error", cause)
	assert.Contains(t, e.Error(), "db connection refused")
	assert.Contains(t, e.Error(), "INTERNAL_ERROR")
}

func TestWrap_Unwrap(t *testing.T) {
	cause := errors.New("root cause")
	e := apperror.Wrap(apperror.ErrInternal, "msg", cause)
	assert.Equal(t, cause, errors.Unwrap(e))
}

func TestNew_ErrorsAs(t *testing.T) {
	e := apperror.New(apperror.ErrNotFound, "not found")
	wrapped := errors.New("wrapped: " + e.Error())
	_ = wrapped

	// errors.As harus resolve ke *apperror.AppError
	var target *apperror.AppError
	require.True(t, errors.As(e, &target))
	assert.Equal(t, apperror.ErrNotFound, target.Code)
}

// ---- Constructor helpers ---------------------------------------------------

func TestValidation(t *testing.T) {
	e := apperror.Validation("field X required")
	assert.Equal(t, apperror.ErrValidation, e.Code)
	assert.Contains(t, e.Message, "field X required")
}

func TestNIKInvalid(t *testing.T) {
	e := apperror.NIKInvalid("1234")
	assert.Equal(t, apperror.ErrNIKInvalid, e.Code)
	assert.Contains(t, e.Message, "1234")
}

func TestQuotaExceeded(t *testing.T) {
	e := apperror.QuotaExceeded()
	assert.Equal(t, apperror.ErrQuotaExceeded, e.Code)
}

func TestStockUnavailable(t *testing.T) {
	e := apperror.StockUnavailable()
	assert.Equal(t, apperror.ErrStockUnavailable, e.Code)
}

func TestTokenInvalid(t *testing.T) {
	e := apperror.TokenInvalid()
	assert.Equal(t, apperror.ErrTokenInvalid, e.Code)
}

func TestTokenExpired(t *testing.T) {
	e := apperror.TokenExpired()
	assert.Equal(t, apperror.ErrTokenExpired, e.Code)
}

func TestBookingExpired(t *testing.T) {
	e := apperror.BookingExpired()
	assert.Equal(t, apperror.ErrBookingExpired, e.Code)
}

func TestSoldOut(t *testing.T) {
	e := apperror.SoldOut()
	assert.Equal(t, apperror.ErrSoldOut, e.Code)
}

func TestNotFound(t *testing.T) {
	e := apperror.NotFound("event")
	assert.Equal(t, apperror.ErrNotFound, e.Code)
	assert.Contains(t, e.Message, "event")
}

func TestInternal(t *testing.T) {
	cause := errors.New("db error")
	e := apperror.Internal(cause)
	assert.Equal(t, apperror.ErrInternal, e.Code)
	assert.Equal(t, cause, errors.Unwrap(e))
}


func TestNew_NoCause_UnwrapNil(t *testing.T) {
	e := apperror.New(apperror.ErrForbidden, "forbidden")
	assert.Nil(t, errors.Unwrap(e))
}

