// Package middleware menyediakan middleware HTTP untuk booking-service.
package middleware

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// responseWriter wraps http.ResponseWriter untuk capture status code.
type responseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func wrapResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{ResponseWriter: w, status: http.StatusOK}
}

func (rw *responseWriter) WriteHeader(code int) {
	if !rw.wroteHeader {
		rw.status = code
		rw.wroteHeader = true
		rw.ResponseWriter.WriteHeader(code)
	}
}

// RequestLogger adalah middleware yang mencatat setiap inbound HTTP request:
//   - method, path, status, duration
//   - X-Transaction-Id (buat baru bila tidak ada di request)
//
// Level: INFO untuk 2xx/3xx, WARN untuk 4xx, ERROR untuk 5xx.
func RequestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// Pastikan transaction ID selalu ada.
			txID := r.Header.Get("X-Transaction-Id")
			if txID == "" {
				txID = uuid.New().String()
			}
			w.Header().Set("X-Transaction-Id", txID)

			wrapped := wrapResponseWriter(w)
			next.ServeHTTP(wrapped, r)

			dur := time.Since(start)
			attrs := []any{
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", wrapped.status),
				slog.String("duration", dur.String()),
				slog.String("txId", txID),
				slog.String("userAgent", r.UserAgent()),
			}

			switch {
			case wrapped.status >= 500:
				log.ErrorContext(r.Context(), "request completed", attrs...)
			case wrapped.status >= 400:
				log.WarnContext(r.Context(), "request completed", attrs...)
			default:
				log.InfoContext(r.Context(), "request completed", attrs...)
			}
		})
	}
}
