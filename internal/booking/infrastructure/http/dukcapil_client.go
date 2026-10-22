package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/refreshTechRusuh/booking-service/internal/config"
)

// dukcapilClient mengimplementasikan outbound.IDukcapilClient.
type dukcapilClient struct {
	baseURL  string
	timeout  time.Duration
	failOpen bool
	log      *slog.Logger
}

func newDukcapilClient(cfg config.DukcapilConfig, log *slog.Logger) *dukcapilClient {
	return &dukcapilClient{
		baseURL:  cfg.BaseURL,
		timeout:  time.Duration(cfg.TimeoutMs) * time.Millisecond,
		failOpen: cfg.FailOpen,
		log:      log,
	}
}

type dukcapilVerifyRequest struct {
	NIK      string `json:"nik"`
	Nama     string `json:"nama"`
	TglLahir string `json:"tglLahir"`
	NamaIbu  string `json:"namaIbu"`
}

type dukcapilVerifyResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Data    struct {
		Valid bool `json:"valid"`
	} `json:"data"`
}

// Verify memanggil POST /internal/dukcapil/verify.
// Jika service down dan failOpen=true → return true + log warning.
func (c *dukcapilClient) Verify(ctx context.Context, nik, nama, tglLahir, namaIbu string) (bool, error) {
	body, _ := json.Marshal(dukcapilVerifyRequest{NIK: nik, Nama: nama, TglLahir: tglLahir, NamaIbu: namaIbu})

	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	resp, err := http.Post(c.baseURL+"/internal/dukcapil/verify", "application/json", bytes.NewReader(body))
	if err != nil {
		if c.failOpen {
			c.log.WarnContext(ctx, "dukcapil service unreachable, fail-open: accepting NIK format check only",
				slog.String("nik", nik), slog.String("error", err.Error()))
			return len(nik) == 16, nil // fail-open: cek format 16 digit saja
		}
		return false, fmt.Errorf("dukcapil client: %w", err)
	}
	defer resp.Body.Close()
	_ = reqCtx

	var result dukcapilVerifyResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		if c.failOpen {
			return len(nik) == 16, nil
		}
		return false, fmt.Errorf("dukcapil decode: %w", err)
	}

	return result.Data.Valid, nil
}
