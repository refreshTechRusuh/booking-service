package kafka

import (
	"context"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafkaHeaderCarrier_GetSetKeys(t *testing.T) {
	headers := []kafkago.Header{}
	c := kafkaHeaderCarrier{headers: &headers}

	// Get dari carrier kosong harus return ""
	assert.Equal(t, "", c.Get("traceparent"), "Get on empty carrier must return empty string")

	// Set satu header
	c.Set("traceparent", "00-abc123-def456-01")
	assert.Equal(t, "00-abc123-def456-01", c.Get("traceparent"))

	// Set header kedua
	c.Set("tracestate", "vendor=value")
	assert.Equal(t, "vendor=value", c.Get("tracestate"))

	// Keys harus mengembalikan semua key
	keys := c.Keys()
	require.Len(t, keys, 2)
	assert.Contains(t, keys, "traceparent")
	assert.Contains(t, keys, "tracestate")
}

func TestKafkaHeaderCarrier_SetOverwrite(t *testing.T) {
	headers := []kafkago.Header{
		{Key: "traceparent", Value: []byte("old-value")},
	}
	c := kafkaHeaderCarrier{headers: &headers}

	// Set dengan key yang sama harus overwrite, bukan append
	c.Set("traceparent", "new-value")

	assert.Equal(t, "new-value", c.Get("traceparent"))
	assert.Len(t, *c.headers, 1, "Set harus overwrite, bukan append header baru")
}

func TestKafkaHeaderCarrier_GetMissing(t *testing.T) {
	headers := []kafkago.Header{
		{Key: "some-key", Value: []byte("some-value")},
	}
	c := kafkaHeaderCarrier{headers: &headers}

	// Get key yang tidak ada harus return ""
	assert.Equal(t, "", c.Get("missing-key"))
}

func TestKafkaHeaderCarrier_KeysEmpty(t *testing.T) {
	headers := []kafkago.Header{}
	c := kafkaHeaderCarrier{headers: &headers}

	keys := c.Keys()
	assert.Empty(t, keys)
}

func TestInjectTraceContext_NoPanic(t *testing.T) {
	// Inject ke message kosong dengan context tanpa span — harus tidak panic.
	// Global propagator default (noop) tidak menghasilkan traceparent,
	// tapi inject tetap harus berjalan tanpa error.
	msg := kafkago.Message{Topic: "test.topic"}
	assert.NotPanics(t, func() {
		InjectTraceContext(context.Background(), &msg)
	})
}

func TestExtractTraceContext_WithTraceparent(t *testing.T) {
	// Simulasi pesan dari producer yang sudah punya traceparent header
	msg := kafkago.Message{
		Topic: "booking.created",
		Headers: []kafkago.Header{
			{Key: "traceparent", Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")},
		},
	}

	// Extract tidak boleh panic dan harus mengembalikan context (bisa noop jika propagator belum di-setup)
	assert.NotPanics(t, func() {
		ctx := ExtractTraceContext(context.Background(), msg)
		assert.NotNil(t, ctx, "ExtractTraceContext harus mengembalikan non-nil context")
	})
}

func TestExtractTraceContext_NoHeaders(t *testing.T) {
	// Pesan tanpa header traceparent — context dikembalikan tanpa modifikasi span
	msg := kafkago.Message{Topic: "payment.succeeded"}
	ctx := ExtractTraceContext(context.Background(), msg)
	assert.NotNil(t, ctx)
}

func TestInjectExtract_RoundTrip(t *testing.T) {
	// Inject ke message, lalu extract — headers harus tersimpan dengan benar
	msg := kafkago.Message{Topic: "booking.created"}

	// Inject (meski tidak ada span aktif, carrier tetap bisa dipanggil)
	InjectTraceContext(context.Background(), &msg)

	// Extract dari message yang sama — tidak boleh panic
	ctx := ExtractTraceContext(context.Background(), msg)
	assert.NotNil(t, ctx)
}
