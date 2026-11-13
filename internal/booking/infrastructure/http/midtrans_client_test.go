package http

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestMidtrans(t *testing.T, httpStatus int, body string) (*MidtransClient, *http.Request) {
	t.Helper()
	var got http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = *r
		w.WriteHeader(httpStatus)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return newMidtransClientWithBaseURL(srv.URL, "server-key", slog.New(slog.NewTextHandler(io.Discard, nil))), &got
}

func TestMidtransExpire_ShouldSucceed_WhenExpiredOrNotFound(t *testing.T) {
	for _, code := range []string{"407", "404"} {
		c, req := newTestMidtrans(t, http.StatusOK, `{"status_code":"`+code+`","status_message":"x"}`)

		err := c.ExpireTransaction(context.Background(), "bkg-1")

		require.NoError(t, err, code)
		assert.Equal(t, http.MethodPost, req.Method)
		assert.Equal(t, "/v2/bkg-1/expire", req.URL.Path)
		user, pass, ok := req.BasicAuth()
		assert.True(t, ok)
		assert.Equal(t, "server-key", user)
		assert.Empty(t, pass)
	}
}

func TestMidtransExpire_ShouldFail_WhenAlreadySettled(t *testing.T) {
	c, _ := newTestMidtrans(t, http.StatusOK, `{"status_code":"412","status_message":"cannot be updated","transaction_status":"settlement"}`)
	err := c.ExpireTransaction(context.Background(), "bkg-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "412")
}

func TestMidtransExpire_ShouldFail_WhenHTTPErrorOrBadBody(t *testing.T) {
	c, _ := newTestMidtrans(t, http.StatusUnauthorized, `not-json`)
	require.Error(t, c.ExpireTransaction(context.Background(), "bkg-1"))
}

func TestNewMidtransClient_ShouldPickBaseURLByEnv(t *testing.T) {
	assert.Equal(t, midtransProductionAPI, NewMidtransClient("k", "production", slog.Default()).baseURL)
	assert.Equal(t, midtransSandboxAPI, NewMidtransClient("k", "sandbox", slog.Default()).baseURL)
}
