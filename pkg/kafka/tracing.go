package kafka

import (
	"context"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/refreshTechRusuh/booking-service/kafka"

// kafkaHeaderCarrier mengadaptasi []kafkago.Header ke propagation.TextMapCarrier
// sehingga W3C traceparent/tracestate bisa di-inject dan di-extract dari Kafka headers.
type kafkaHeaderCarrier struct {
	headers *[]kafkago.Header
}

// Get mengembalikan nilai header pertama yang cocok dengan key.
func (c kafkaHeaderCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set menambah atau mengganti header dengan key yang diberikan.
func (c kafkaHeaderCarrier) Set(key, val string) {
	for i, h := range *c.headers {
		if h.Key == key {
			(*c.headers)[i].Value = []byte(val)
			return
		}
	}
	*c.headers = append(*c.headers, kafkago.Header{Key: key, Value: []byte(val)})
}

// Keys mengembalikan semua key yang ada di headers.
func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, len(*c.headers))
	for i, h := range *c.headers {
		keys[i] = h.Key
	}
	return keys
}

// InjectTraceContext menyisipkan W3C traceparent ke headers pesan Kafka.
// Dipanggil sebelum WriteMessages agar consumer bisa menyambung trace yang sama.
func InjectTraceContext(ctx context.Context, msg *kafkago.Message) {
	otel.GetTextMapPropagator().Inject(ctx, kafkaHeaderCarrier{headers: &msg.Headers})
}

// ExtractTraceContext mengambil trace context dari headers pesan Kafka yang diterima.
// Mengembalikan context baru yang sudah mengandung parent span dari producer.
func ExtractTraceContext(ctx context.Context, msg kafkago.Message) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, kafkaHeaderCarrier{headers: &msg.Headers})
}

// StartProduceSpan membuka span untuk kafka.produce.
// Caller harus memanggil span.End() setelah WriteMessages selesai.
func StartProduceSpan(ctx context.Context, topic string) (context.Context, trace.Span) {
	tracer := otel.GetTracerProvider().Tracer(tracerName)
	return tracer.Start(ctx, "kafka.produce "+topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			attribute.String("messaging.destination", topic),
		),
	)
}

// StartConsumeSpan membuka span untuk kafka.receive.
// Harus dipanggil setelah ExtractTraceContext agar span terhubung ke producer span.
// Caller harus memanggil span.End() setelah pesan selesai diproses.
func StartConsumeSpan(ctx context.Context, topic string) (context.Context, trace.Span) {
	tracer := otel.GetTracerProvider().Tracer(tracerName)
	return tracer.Start(ctx, "kafka.receive "+topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			attribute.String("messaging.destination", topic),
		),
	)
}
