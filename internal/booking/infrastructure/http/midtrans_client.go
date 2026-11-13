package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"
)

const (
	midtransSandboxAPI    = "https://api.sandbox.midtrans.com"
	midtransProductionAPI = "https://api.midtrans.com"
)

// MidtransClient memanggil Midtrans Core API untuk meng-expire transaksi.
// Mengimplementasikan outbound.IPaymentTransactionCanceller.
type MidtransClient struct {
	baseURL   string
	serverKey string
	client    *http.Client
	log       *slog.Logger
}

// NewMidtransClient membuat client; env "production" memakai API produksi, selain itu sandbox.
func NewMidtransClient(serverKey, env string, log *slog.Logger) *MidtransClient {
	base := midtransSandboxAPI
	if env == "production" {
		base = midtransProductionAPI
	}
	return newMidtransClientWithBaseURL(base, serverKey, log)
}

func newMidtransClientWithBaseURL(baseURL, serverKey string, log *slog.Logger) *MidtransClient {
	return &MidtransClient{baseURL: baseURL, serverKey: serverKey, client: &http.Client{Timeout: 8 * time.Second}, log: log}
}

// midtransStatus adalah bagian respons Midtrans yang kita perlukan.
// Midtrans membalas HTTP 200 dengan status_code di body (mis. "407" = sudah expired).
type midtransStatus struct {
	StatusCode        string `json:"status_code"`
	StatusMessage     string `json:"status_message"`
	TransactionStatus string `json:"transaction_status"`
}

// ExpireTransaction memanggil POST /v2/{order_id}/expire.
//   - 407 (expired) / 404 (transaksi belum ada: Snap dibuka tapi metode belum dipilih) = sukses.
//   - status lain (mis. 412: sudah settlement) dikembalikan sebagai error untuk dicatat.
func (c *MidtransClient) ExpireTransaction(ctx context.Context, orderID string) error {
	endpoint := fmt.Sprintf("%s/v2/%s/expire", c.baseURL, url.PathEscape(orderID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return fmt.Errorf("midtrans expire: build request: %w", err)
	}
	req.SetBasicAuth(c.serverKey, "")
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("midtrans expire: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	var st midtransStatus
	_ = json.Unmarshal(body, &st) // body kosong / bukan JSON → StatusCode kosong, ditangani di bawah

	switch {
	case st.StatusCode == "407", st.StatusCode == "404":
		c.log.InfoContext(ctx, "midtrans transaction expired",
			slog.String("orderID", orderID), slog.String("statusCode", st.StatusCode))
		return nil
	case resp.StatusCode >= 300 || st.StatusCode == "":
		return fmt.Errorf("midtrans expire: http %d status %q", resp.StatusCode, st.StatusCode)
	default:
		return fmt.Errorf("midtrans expire: status %s (%s) tx=%s",
			st.StatusCode, st.StatusMessage, st.TransactionStatus)
	}
}
