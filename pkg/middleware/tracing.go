package middleware

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// OtelMiddleware membungkus handler dengan OpenTelemetry HTTP instrumentation.
// Setiap inbound request otomatis mendapat span dengan atribut:
//   - http.method, http.route, http.status_code, http.url
//   - W3C traceparent/tracestate header dari FE diteruskan sebagai parent span.
//
// Harus di-apply SETELAH RequestLogger agar txId sudah ada di context.
func OtelMiddleware(serviceName string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return otelhttp.NewHandler(next, serviceName,
			otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
		)
	}
}
