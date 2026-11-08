package http_test

// stream_leak_test.go — tes goroutine leak dan success path untuk StreamStock SSE handler.
//
// Pola SSE yang berpotensi leak:
//   1. Mock goroutine yang menulis ke channel sedangkan ctx sudah dicancel (handler sudah exit).
//   2. Handler goroutine yang menunggu di `select` ketika channel tidak pernah ditutup.
//
// Kedua kasus harus clean — diverifikasi dengan goleak dan goroutine count check.

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/refreshTechRusuh/booking-service/internal/booking/domain/dto"
	"github.com/stretchr/testify/assert"
)

// TestMain mengaktifkan goleak untuk seluruh test package ini.
// Setiap test akan diverifikasi tidak meninggalkan goroutine yang berjalan setelah selesai.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// ── Success path ──────────────────────────────────────────────────────────────

// TestStreamStock_Success_CloseImmediately: channel ditutup segera → handler harus return clean.
func TestStreamStock_Success_CloseImmediately(t *testing.T) {
	ch := make(chan dto.StockBatchEvent)
	close(ch) // tutup SEBELUM handler mulai membaca

	uc := &mockBookingUsecase{
		streamStockFn: func(_ context.Context, _ string) (<-chan dto.StockBatchEvent, error) {
			return ch, nil
		},
	}
	mux := setupBookingRouter(uc)

	ttl := time.Now().Add(5 * time.Minute).Unix()
	token := makeAccessToken(t, "bkg-001", "evt-001", ttl)
	req := httptest.NewRequest(http.MethodGet, "/api/booking/v1/event/evt-001/stock/stream", nil)
	req.Header.Set("X-Access-Token", token)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	// Handler harus return segera setelah membaca channel closed.
	assert.Equal(t, http.StatusOK, rr.Code) // 200 karena header SSE sudah dikirim sebelum channel dibaca
	// Body harus mengandung SSE "connected" comment
	assert.Contains(t, rr.Body.String(), ": connected to stock stream")
}

// TestStreamStock_Success_SendOneEvent: satu event dikirim lalu channel ditutup.
func TestStreamStock_Success_SendOneEvent(t *testing.T) {
	event := dto.StockBatchEvent{
		EventID: "evt-001",
		Categories: []dto.StockEvent{
			{CategoryID: "cat-1", Available: 45},
		},
	}

	// Buffered channel: pre-load event, tutup — handler baca lalu exit
	ch := make(chan dto.StockBatchEvent, 1)
	ch <- event
	close(ch)

	uc := &mockBookingUsecase{
		streamStockFn: func(_ context.Context, _ string) (<-chan dto.StockBatchEvent, error) {
			return ch, nil
		},
	}
	mux := setupBookingRouter(uc)

	ttl := time.Now().Add(5 * time.Minute).Unix()
	token := makeAccessToken(t, "bkg-001", "evt-001", ttl)
	req := httptest.NewRequest(http.MethodGet, "/api/booking/v1/event/evt-001/stock/stream", nil)
	req.Header.Set("X-Access-Token", token)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	body := rr.Body.String()
	assert.Contains(t, body, "event: stock")
	assert.Contains(t, body, `"eventId":"evt-001"`)
	assert.Contains(t, body, `"categoryId":"cat-1"`)
}

// TestStreamStock_Success_SendMultipleEvents: beberapa event dikirim lalu channel ditutup.
func TestStreamStock_Success_SendMultipleEvents(t *testing.T) {
	ch := make(chan dto.StockBatchEvent, 3)
	ch <- dto.StockBatchEvent{EventID: "evt-1", Categories: []dto.StockEvent{{CategoryID: "cat-1", Available: 100}}}
	ch <- dto.StockBatchEvent{EventID: "evt-1", Categories: []dto.StockEvent{{CategoryID: "cat-1", Available: 98}}}
	ch <- dto.StockBatchEvent{EventID: "evt-1", SoldOut: true}
	close(ch)

	uc := &mockBookingUsecase{
		streamStockFn: func(_ context.Context, _ string) (<-chan dto.StockBatchEvent, error) {
			return ch, nil
		},
	}
	mux := setupBookingRouter(uc)

	ttl := time.Now().Add(5 * time.Minute).Unix()
	token := makeAccessToken(t, "bkg-001", "evt-1", ttl)
	req := httptest.NewRequest(http.MethodGet, "/api/booking/v1/event/evt-1/stock/stream", nil)
	req.Header.Set("X-Access-Token", token)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	body := rr.Body.String()
	lines := strings.Split(body, "\n")
	eventLines := 0
	for _, l := range lines {
		if strings.HasPrefix(l, "event: stock") {
			eventLines++
		}
	}
	assert.Equal(t, 3, eventLines, "harus ada 3 frame SSE dikirim")
}

// TestStreamStock_SSEFormat: verifikasi format SSE sesuai spec (event: stock\ndata: ...\n\n).
func TestStreamStock_SSEFormat(t *testing.T) {
	ch := make(chan dto.StockBatchEvent, 1)
	ch <- dto.StockBatchEvent{EventID: "evt-format", Categories: []dto.StockEvent{{CategoryID: "cat-x", Available: 50}}}
	close(ch)

	uc := &mockBookingUsecase{
		streamStockFn: func(_ context.Context, _ string) (<-chan dto.StockBatchEvent, error) {
			return ch, nil
		},
	}
	mux := setupBookingRouter(uc)

	ttl := time.Now().Add(5 * time.Minute).Unix()
	token := makeAccessToken(t, "bkg-001", "evt-format", ttl)
	req := httptest.NewRequest(http.MethodGet, "/api/booking/v1/event/evt-format/stock/stream", nil)
	req.Header.Set("X-Access-Token", token)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	// Verifikasi header SSE
	assert.Equal(t, "text/event-stream", rr.Header().Get("Content-Type"))
	assert.Equal(t, "no-cache", rr.Header().Get("Cache-Control"))
	assert.Equal(t, "keep-alive", rr.Header().Get("Connection"))

	// Parse SSE body
	scanner := bufio.NewScanner(strings.NewReader(rr.Body.String()))
	var foundEventLine, foundDataLine bool
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "event: stock") {
			foundEventLine = true
		}
		if strings.HasPrefix(line, "data: ") && strings.Contains(line, "evt-format") {
			foundDataLine = true
		}
	}
	assert.True(t, foundEventLine, "harus ada baris 'event: stock'")
	assert.True(t, foundDataLine, "harus ada baris 'data: ...'")
}

// ── Goroutine leak test: context cancellation ────────────────────────────────

// TestStreamStock_ContextCancel_NoLeak: paling penting — client disconnect tidak meninggalkan goroutine.
//
// Skenario: mock mengirim event lewat goroutine yang AWARE terhadap context.
// Saat ctx dicancel (simulasi client disconnect), goroutine mock harus ikut exit.
func TestStreamStock_ContextCancel_NoLeak(t *testing.T) {
	// Goroutine count sebelum test
	goroutinesBefore := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())

	// Channel unbuffered — goroutine pengirim akan block di send
	// sampai handler membaca ATAU context dicancel.
	ch := make(chan dto.StockBatchEvent)

	// Goroutine mock yang BERHENTI saat ctx dicancel — mencegah leak.
	go func() {
		defer close(ch)
		for i := 0; i < 100; i++ {
			select {
			case <-ctx.Done():
				return // exit bersama context
			case ch <- dto.StockBatchEvent{EventID: "evt-cancel"}:
				// event terkirim
			}
		}
	}()

	uc := &mockBookingUsecase{
		streamStockFn: func(c context.Context, _ string) (<-chan dto.StockBatchEvent, error) {
			return ch, nil
		},
	}
	mux := setupBookingRouter(uc)

	ttl := time.Now().Add(5 * time.Minute).Unix()
	token := makeAccessToken(t, "bkg-001", "evt-cancel", ttl)
	req := httptest.NewRequest(http.MethodGet, "/api/booking/v1/event/evt-cancel/stock/stream", nil)
	req.Header.Set("X-Access-Token", token)
	req = req.WithContext(ctx)
	rr := httptest.NewRecorder()

	// Jalankan handler di goroutine terpisah karena akan block menunggu channel/ctx
	done := make(chan struct{})
	go func() {
		defer close(done)
		mux.ServeHTTP(rr, req)
	}()

	// Beri handler waktu untuk membaca beberapa event
	time.Sleep(10 * time.Millisecond)

	// Cancel context — simulasi client disconnect
	cancel()

	// Tunggu handler selesai
	select {
	case <-done:
		// Handler selesai clean ✓
	case <-time.After(2 * time.Second):
		t.Fatal("handler tidak selesai dalam 2 detik setelah context cancel — kemungkinan goroutine leak")
	}

	// Beri waktu goroutine mock untuk exit
	time.Sleep(50 * time.Millisecond)

	// Goroutine count setelah test harus kembali ke baseline (max +2 toleransi)
	goroutinesAfter := runtime.NumGoroutine()
	leaked := goroutinesAfter - goroutinesBefore
	assert.LessOrEqual(t, leaked, 2,
		"goroutine setelah test: %d, sebelum: %d — kemungkinan ada leak: %d goroutine",
		goroutinesAfter, goroutinesBefore, leaked)
}

// TestStreamStock_ContextAlreadyCancelled: ctx sudah dicancel sebelum request diproses.
func TestStreamStock_ContextAlreadyCancelled(t *testing.T) {
	// Ini test paling agresif: context dicancel SEBELUM handler dipanggil.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel langsung

	ch := make(chan dto.StockBatchEvent)
	go func() {
		defer close(ch)
		// ctx sudah done, goroutine exit segera
		select {
		case <-ctx.Done():
			return
		}
	}()

	uc := &mockBookingUsecase{
		streamStockFn: func(c context.Context, _ string) (<-chan dto.StockBatchEvent, error) {
			return ch, nil
		},
	}
	mux := setupBookingRouter(uc)

	ttl := time.Now().Add(5 * time.Minute).Unix()
	token := makeAccessToken(t, "bkg-001", "evt-precancel", ttl)
	req := httptest.NewRequest(http.MethodGet, "/api/booking/v1/event/evt-precancel/stock/stream", nil)
	req.Header.Set("X-Access-Token", token)
	req = req.WithContext(ctx)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)
	// Handler harus selesai (tanpa hang) karena ctx sudah done atau ch closed
}

// ── Handler detail tests ──────────────────────────────────────────────────────

// TestStreamStock_SoldOutEvent: event sold-out melalui channel harus dikirim lalu stream selesai.
func TestStreamStock_SoldOutEvent(t *testing.T) {
	ch := make(chan dto.StockBatchEvent, 1)
	ch <- dto.StockBatchEvent{EventID: "evt-sold", SoldOut: true}
	close(ch)

	uc := &mockBookingUsecase{
		streamStockFn: func(_ context.Context, _ string) (<-chan dto.StockBatchEvent, error) {
			return ch, nil
		},
	}
	mux := setupBookingRouter(uc)

	ttl := time.Now().Add(5 * time.Minute).Unix()
	token := makeAccessToken(t, "bkg-001", "evt-sold", ttl)
	req := httptest.NewRequest(http.MethodGet, "/api/booking/v1/event/evt-sold/stock/stream", nil)
	req.Header.Set("X-Access-Token", token)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	body := rr.Body.String()
	assert.Contains(t, body, `"soldOut":true`)
}

// TestStreamStock_EmptyEventID: eventId kosong harus return 400.
func TestStreamStock_EmptyEventID(t *testing.T) {
	mux := setupBookingRouter(&mockBookingUsecase{})

	ttl := time.Now().Add(5 * time.Minute).Unix()
	// Gunakan path tanpa eventId — handler harus menangani
	token := makeAccessToken(t, "bkg-001", "", ttl)
	req := httptest.NewRequest(http.MethodGet, "/api/booking/v1/event//stock/stream", nil)
	req.Header.Set("X-Access-Token", token)
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	// Path matcher tidak cocok → 404 atau 400
	assert.NotEqual(t, http.StatusOK, rr.Code)
}

// ── Regression: bukan leak dari mock default ─────────────────────────────────

// TestStreamStock_DefaultMock_NoLeak: verifikasi mock default (go func close) tidak leak.
func TestStreamStock_DefaultMock_NoLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	uc := &mockBookingUsecase{} // default StreamStock: close ch immediately

	mux := setupBookingRouter(uc)

	// Panggil beberapa kali untuk memverifikasi tidak ada akumulasi goroutine
	for i := 0; i < 5; i++ {
		ttl := time.Now().Add(5 * time.Minute).Unix()
		token := makeAccessToken(t, "bkg-001", "evt-001", ttl)
		req := httptest.NewRequest(http.MethodGet, "/api/booking/v1/event/evt-001/stock/stream", nil)
		req.Header.Set("X-Access-Token", token)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
	}

	// Beri waktu goroutine cleanup
	time.Sleep(10 * time.Millisecond)

	after := runtime.NumGoroutine()
	assert.LessOrEqual(t, after-before, 2,
		"5 panggilan StreamStock harus tidak meninggalkan goroutine (got %d extra)", after-before)
}
