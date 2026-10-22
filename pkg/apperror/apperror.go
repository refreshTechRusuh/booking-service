// Package apperror mendefinisikan tipe error domain booking-service.
// Pola diambil dari service-dashboard pkg/apperror.
package apperror

import "fmt"

// ErrorCode adalah kode error domain yang dipetakan ke HTTP status di apinizer.
type ErrorCode string

const (
	// 400
	ErrValidation     ErrorCode = "VALIDATION_ERROR"
	ErrNIKInvalid     ErrorCode = "NIK_INVALID"
	ErrQuotaExceeded  ErrorCode = "QUOTA_EXCEEDED"

	// 401
	ErrUnauthorized   ErrorCode = "UNAUTHORIZED"
	ErrTokenInvalid   ErrorCode = "TOKEN_INVALID"

	// 403
	ErrForbidden      ErrorCode = "FORBIDDEN"

	// 404
	ErrNotFound       ErrorCode = "NOT_FOUND"

	// 409
	ErrStockUnavailable ErrorCode = "STOCK_UNAVAILABLE"
	ErrQueueDuplicate   ErrorCode = "QUEUE_DUPLICATE"

	// 410
	ErrTokenExpired     ErrorCode = "TOKEN_EXPIRED"
	ErrBookingExpired   ErrorCode = "BOOKING_EXPIRED"
	ErrSoldOut          ErrorCode = "SOLD_OUT"

	// 422
	ErrMethodTimeInsufficient ErrorCode = "METHOD_TIME_INSUFFICIENT"

	// 429
	ErrRateLimited ErrorCode = "RATE_LIMITED"

	// 500
	ErrInternal ErrorCode = "INTERNAL_ERROR"
)

// AppError adalah error domain dengan kode spesifik + pesan human-readable.
// Bungkus dengan errors.As di error_mapper agar bisa di-unwrap ke AppError.
type AppError struct {
	Code    ErrorCode
	Message string
	cause   error
}

func New(code ErrorCode, message string) *AppError {
	return &AppError{Code: code, Message: message}
}

func Wrap(code ErrorCode, message string, cause error) *AppError {
	return &AppError{Code: code, Message: message, cause: cause}
}

func (e *AppError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.cause)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

func (e *AppError) Unwrap() error { return e.cause }

// ---- Constructor helpers -----------------------------------------------

func Validation(msg string) *AppError        { return New(ErrValidation, msg) }
func NIKInvalid(nik string) *AppError        { return New(ErrNIKInvalid, "NIK tidak valid: "+nik) }
func QuotaExceeded() *AppError               { return New(ErrQuotaExceeded, "Melebihi batas 3 tiket per NIK untuk event ini") }
func StockUnavailable() *AppError            { return New(ErrStockUnavailable, "Stok tidak mencukupi") }
func TokenInvalid() *AppError                { return New(ErrTokenInvalid, "Access Token tidak valid") }
func TokenExpired() *AppError                { return New(ErrTokenExpired, "Waktu booking telah habis, silakan mulai ulang") }
func BookingExpired() *AppError              { return New(ErrBookingExpired, "Waktu pembayaran telah habis") }
func SoldOut() *AppError                     { return New(ErrSoldOut, "Tiket sudah habis terjual") }
func MethodTimeInsufficient() *AppError      { return New(ErrMethodTimeInsufficient, "Sisa waktu tidak cukup untuk metode ini") }
func NotFound(resource string) *AppError     { return New(ErrNotFound, resource+" tidak ditemukan") }
func Internal(cause error) *AppError         { return Wrap(ErrInternal, "Terjadi kesalahan pada server", cause) }
