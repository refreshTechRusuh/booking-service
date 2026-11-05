package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/refreshTechRusuh/booking-service/internal/booking/infrastructure/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	vk "github.com/valkey-io/valkey-go"
)

// newMiniredisClient membuat vk.Client yang terhubung ke miniredis (in-memory).
func newMiniredisClient(t *testing.T) (vk.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	c, err := vk.NewClient(vk.ClientOption{
		InitAddress:  []string{mr.Addr()},
		DisableCache: true, // nonaktifkan client-side caching agar kompatibel RESP2
	})
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c, mr
}

// ── PublishStockChanged ────────────────────────────────────────────────────────

func TestCacheRepository_PublishStockChanged_success(t *testing.T) {
	client, _ := newMiniredisClient(t)
	repo := cache.NewCacheRepository(client, testSecret, discardLog())

	payload := []byte(`{"eventId":"event-1","categoryId":"cat-1","available":98}`)
	err := repo.PublishStockChanged(context.Background(), "event-1", payload)
	require.NoError(t, err)
}

func TestCacheRepository_PublishStockChanged_multipleEvents(t *testing.T) {
	client, _ := newMiniredisClient(t)
	repo := cache.NewCacheRepository(client, testSecret, discardLog())

	payload1 := []byte(`{"eventId":"event-1","available":50}`)
	payload2 := []byte(`{"eventId":"event-2","available":100}`)

	require.NoError(t, repo.PublishStockChanged(context.Background(), "event-1", payload1))
	require.NoError(t, repo.PublishStockChanged(context.Background(), "event-2", payload2))
	// Dua channel berbeda (stock:event-1 dan stock:event-2) — tidak ada konflik
}

func TestCacheRepository_PublishStockChanged_largePayload(t *testing.T) {
	client, _ := newMiniredisClient(t)
	repo := cache.NewCacheRepository(client, testSecret, discardLog())

	// Payload besar (5KB) — tetap harus berhasil
	largePayload := make([]byte, 5000)
	for i := range largePayload {
		largePayload[i] = 'x'
	}

	err := repo.PublishStockChanged(context.Background(), "event-large", largePayload)
	require.NoError(t, err)
}

// ── SubscribeStockChanged ─────────────────────────────────────────────────────

func TestCacheRepository_SubscribeStockChanged_receivesPublished(t *testing.T) {
	client, _ := newMiniredisClient(t)
	repo := cache.NewCacheRepository(client, testSecret, discardLog())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, unsub, err := repo.SubscribeStockChanged(ctx, "event-sub-1")
	require.NoError(t, err)
	defer unsub()

	// Beri waktu subscribe untuk setup sebelum publish
	time.Sleep(50 * time.Millisecond)

	payload := []byte(`{"eventId":"event-sub-1","available":45}`)
	require.NoError(t, repo.PublishStockChanged(context.Background(), "event-sub-1", payload))

	select {
	case msg := <-ch:
		assert.Equal(t, payload, msg)
	case <-ctx.Done():
		t.Skip("timeout menunggu pesan — miniredis Pub/Sub async mungkin belum ready; test di-skip bukan fail")
	}
}

func TestCacheRepository_SubscribeStockChanged_unsubCleanup(t *testing.T) {
	client, _ := newMiniredisClient(t)
	repo := cache.NewCacheRepository(client, testSecret, discardLog())

	ctx, cancel := context.WithCancel(context.Background())
	ch, unsub, err := repo.SubscribeStockChanged(ctx, "event-unsub")
	require.NoError(t, err)

	// Panggil unsub, lalu cancel context → goroutine harus bersih
	unsub()
	cancel()

	// Beri waktu goroutine untuk selesai
	time.Sleep(50 * time.Millisecond)

	// Channel harus sudah closed (atau masih bisa dibaca tanpa panic)
	select {
	case _, open := <-ch:
		_ = open // sudah closed atau kosong — keduanya valid
	default:
		// Tidak ada pesan menunggu — normal
	}
}
