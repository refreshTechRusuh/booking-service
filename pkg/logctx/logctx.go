// Package logctx menyediakan korelasi txId antar-log: txId disimpan di context,
// lalu ContextHandler menambahkannya otomatis ke SETIAP record slog yang dibuat
// via *Context(...). Dengan ini satu txId menyambungkan seluruh log satu request —
// dari inbound HTTP sampai query DB — tanpa perlu menambah field txId manual di
// tiap pemanggilan log.
package logctx

import (
	"context"
	"log/slog"
)

type ctxKey struct{}

var txKey = ctxKey{}

// WithTxID menitipkan txId ke dalam context.
func WithTxID(ctx context.Context, txID string) context.Context {
	return context.WithValue(ctx, txKey, txID)
}

// TxID membaca txId dari context ("" bila tidak ada).
func TxID(ctx context.Context) string {
	if v, ok := ctx.Value(txKey).(string); ok {
		return v
	}
	return ""
}

// ContextHandler membungkus slog.Handler dan menyuntikkan txId (bila ada di
// context) ke setiap record. Semua log via *Context(ctx, ...) otomatis membawa txId.
type ContextHandler struct {
	slog.Handler
}

func NewContextHandler(h slog.Handler) *ContextHandler {
	return &ContextHandler{Handler: h}
}

func (h *ContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if txID := TxID(ctx); txID != "" {
		r.AddAttrs(slog.String("txId", txID))
	}
	return h.Handler.Handle(ctx, r)
}

func (h *ContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ContextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h *ContextHandler) WithGroup(name string) slog.Handler {
	return &ContextHandler{Handler: h.Handler.WithGroup(name)}
}
