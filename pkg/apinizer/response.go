// Package apinizer menyediakan envelope response standar service-dashboard.
// { status, message, data } — status "00000" sukses, string kode HTTP gagal.
// Transaction ID di response header X-Transaction-Id.
package apinizer

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/refreshTechRusuh/booking-service/pkg/apperror"
)

// Envelope adalah wrapper response tunggal untuk semua endpoint.
type Envelope struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Data    any    `json:"data"`
	Partial bool   `json:"partial,omitempty"`
}

// Success menulis response 200 OK dengan payload data.
func Success(w http.ResponseWriter, message string, data any) {
	writeJSON(w, http.StatusOK, Envelope{Status: "00000", Message: message, Data: data})
}

// Created menulis response 200 OK (sesuai openapi.yaml POST /bookings = 200).
func Created(w http.ResponseWriter, message string, data any) {
	writeJSON(w, http.StatusOK, Envelope{Status: "00000", Message: message, Data: data})
}

// SuccessPartial menulis response 200 dengan partial=true.
func SuccessPartial(w http.ResponseWriter, message string, data any) {
	writeJSON(w, http.StatusOK, Envelope{Status: "00000", Message: message, Data: data, Partial: true})
}

// WriteError memetakan AppError ke HTTP status + envelope gagal.
func WriteError(w http.ResponseWriter, err error) {
	var appErr *apperror.AppError
	if !errors.As(err, &appErr) {
		// Bungkus error tak terduga sebagai 500 — detail disembunyikan dari klien.
		slog.Error("unhandled error", "err", err)
		writeJSON(w, http.StatusInternalServerError, Envelope{
			Status:  "500",
			Message: "Terjadi kesalahan pada server",
			Data:    struct{}{},
		})
		return
	}

	status, message := mapErrorCode(appErr.Code, appErr.Message)
	writeJSON(w, status, Envelope{Status: httpStatusStr(status), Message: message, Data: struct{}{}})
}

// mapErrorCode memetakan ErrorCode ke (httpStatus, userMessage).
func mapErrorCode(code apperror.ErrorCode, msg string) (int, string) {
	switch code {
	case apperror.ErrValidation, apperror.ErrNIKInvalid:
		return http.StatusBadRequest, msg
	case apperror.ErrUnauthorized, apperror.ErrTokenInvalid:
		return http.StatusUnauthorized, msg
	case apperror.ErrForbidden:
		return http.StatusForbidden, msg
	case apperror.ErrNotFound:
		return http.StatusNotFound, msg
	case apperror.ErrStockUnavailable, apperror.ErrQuotaExceeded, apperror.ErrQueueDuplicate,
		apperror.ErrMethodTimeInsufficient:
		return http.StatusConflict, msg
	case apperror.ErrTokenExpired, apperror.ErrBookingExpired, apperror.ErrSoldOut:
		return http.StatusGone, msg
	case apperror.ErrRateLimited:
		return http.StatusTooManyRequests, msg
	default:
		slog.Error("unmapped error code", "code", code, "msg", msg)
		return http.StatusInternalServerError, "Terjadi kesalahan pada server"
	}
}

func httpStatusStr(status int) string {
	switch status {
	case 400:
		return "400"
	case 401:
		return "401"
	case 403:
		return "403"
	case 404:
		return "404"
	case 409:
		return "409"
	case 410:
		return "410"
	case 422:
		return "422"
	case 429:
		return "429"
	default:
		return "500"
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Error("writeJSON encode error", "err", err)
	}
}
