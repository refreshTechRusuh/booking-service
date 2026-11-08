package config_test

import (
	"fmt"
	"testing"

	"github.com/refreshTechRusuh/booking-service/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setRequired menset env var JWT yang wajib agar Load() tidak os.Exit.
func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("ACCESS_TOKEN_SECRET", "access-secret-for-test")
	t.Setenv("PAYMENT_TOKEN_SECRET", "payment-secret-for-test")
}

// ── Load defaults ─────────────────────────────────────────────────────────────

func TestLoad_defaults(t *testing.T) {
	setRequired(t)

	cfg, err := config.Load()
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, "local", cfg.App.Env)
	assert.Equal(t, 8081, cfg.App.Port)
	assert.Equal(t, "info", cfg.Log.Level)
	assert.Equal(t, "json", cfg.Log.Format)

	// DB defaults
	assert.Equal(t, "localhost", cfg.DB.Host)
	assert.Equal(t, 5432, cfg.DB.Port)
	assert.Equal(t, "postgres", cfg.DB.User)
	assert.Equal(t, "rusuh_booking", cfg.DB.Name)
	assert.Equal(t, "disable", cfg.DB.SSLMode)
	assert.Equal(t, 25, cfg.DB.MaxOpenConns)
	assert.Equal(t, 10, cfg.DB.MaxIdleConns)
	assert.Equal(t, 5, cfg.DB.ConnTimeoutSecs)

	// Valkey defaults
	assert.Equal(t, "localhost:6379", cfg.Valkey.Addr)
	assert.False(t, cfg.Valkey.TLS)

	// Kafka defaults
	assert.Equal(t, "localhost:9092", cfg.Kafka.Brokers)
	assert.False(t, cfg.Kafka.TLS)

	// JWT set via t.Setenv
	assert.Equal(t, "access-secret-for-test", cfg.JWT.AccessTokenSecret)
	assert.Equal(t, "payment-secret-for-test", cfg.JWT.PaymentTokenSecret)

	// Dukcapil defaults
	assert.Equal(t, "http://localhost:8085", cfg.Dukcapil.BaseURL)
	assert.Equal(t, 3000, cfg.Dukcapil.TimeoutMs)
	assert.True(t, cfg.Dukcapil.FailOpen)

	// SSE defaults
	assert.Equal(t, 30, cfg.SSE.HeartbeatSeconds)
}

func TestLoad_customAppPort(t *testing.T) {
	setRequired(t)
	t.Setenv("APP_PORT", "9090")

	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, 9090, cfg.App.Port)
}

func TestLoad_PORTEnvTakesPrecedence(t *testing.T) {
	setRequired(t)
	t.Setenv("PORT", "8000")
	t.Setenv("APP_PORT", "9090")

	cfg, err := config.Load()
	require.NoError(t, err)
	// PORT harus lebih prioritas dari APP_PORT
	assert.Equal(t, 8000, cfg.App.Port)
}

func TestLoad_customLogLevel(t *testing.T) {
	setRequired(t)
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("LOG_FORMAT", "text")

	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "debug", cfg.Log.Level)
	assert.Equal(t, "text", cfg.Log.Format)
}

func TestLoad_customDB(t *testing.T) {
	setRequired(t)
	t.Setenv("DB_HOST", "10.0.0.1")
	t.Setenv("DB_PORT", "5433")
	t.Setenv("DB_USER", "admin")
	t.Setenv("DB_NAME", "mydb")
	t.Setenv("DB_SSL_MODE", "require")
	t.Setenv("DB_MAX_OPEN_CONNS", "50")
	t.Setenv("DB_MAX_IDLE_CONNS", "20")
	t.Setenv("DB_CONN_TIMEOUT_SECONDS", "10")

	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.1", cfg.DB.Host)
	assert.Equal(t, 5433, cfg.DB.Port)
	assert.Equal(t, "admin", cfg.DB.User)
	assert.Equal(t, "mydb", cfg.DB.Name)
	assert.Equal(t, "require", cfg.DB.SSLMode)
	assert.Equal(t, 50, cfg.DB.MaxOpenConns)
	assert.Equal(t, 20, cfg.DB.MaxIdleConns)
	assert.Equal(t, 10, cfg.DB.ConnTimeoutSecs)
}

func TestLoad_customValkey(t *testing.T) {
	setRequired(t)
	t.Setenv("VALKEY_ADDR", "redis.internal:6380")
	t.Setenv("VALKEY_PASSWORD", "secret123")
	t.Setenv("VALKEY_TLS", "true")

	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "redis.internal:6380", cfg.Valkey.Addr)
	assert.Equal(t, "secret123", cfg.Valkey.Password)
	assert.True(t, cfg.Valkey.TLS)
}

func TestLoad_customKafka(t *testing.T) {
	setRequired(t)
	t.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")
	t.Setenv("KAFKA_USERNAME", "kafka-user")
	t.Setenv("KAFKA_TLS", "true")

	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "broker1:9092,broker2:9092", cfg.Kafka.Brokers)
	assert.Equal(t, "kafka-user", cfg.Kafka.Username)
	assert.True(t, cfg.Kafka.TLS)
}

func TestLoad_customDukcapil(t *testing.T) {
	setRequired(t)
	t.Setenv("DUKCAPIL_BASE_URL", "http://dukcapil.internal:8085")
	t.Setenv("DUKCAPIL_TIMEOUT_MS", "5000")
	t.Setenv("DUKCAPIL_FAIL_OPEN", "false")

	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "http://dukcapil.internal:8085", cfg.Dukcapil.BaseURL)
	assert.Equal(t, 5000, cfg.Dukcapil.TimeoutMs)
	assert.False(t, cfg.Dukcapil.FailOpen)
}

func TestLoad_customSSE(t *testing.T) {
	setRequired(t)
	t.Setenv("SSE_HEARTBEAT_SECONDS", "60")

	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, 60, cfg.SSE.HeartbeatSeconds)
}

func TestLoad_customEventServiceURL(t *testing.T) {
	setRequired(t)
	t.Setenv("EVENT_SERVICE_URL", "https://event-service-xyz.run.app")

	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "https://event-service-xyz.run.app", cfg.EventSvc.BaseURL)
}

// ── DSN ──────────────────────────────────────────────────────────────────────

func TestDBConfig_DSN_format(t *testing.T) {
	setRequired(t)
	t.Setenv("DB_HOST", "db-host")
	t.Setenv("DB_PORT", "5432")
	t.Setenv("DB_USER", "user1")
	t.Setenv("DB_PASSWORD", "pass1")
	t.Setenv("DB_NAME", "db1")
	t.Setenv("DB_SSL_MODE", "require")
	t.Setenv("DB_CONN_TIMEOUT_SECONDS", "5")

	cfg, err := config.Load()
	require.NoError(t, err)

	dsn := cfg.DB.DSN()
	assert.Contains(t, dsn, "host=db-host")
	assert.Contains(t, dsn, "port=5432")
	assert.Contains(t, dsn, "user=user1")
	assert.Contains(t, dsn, "password=pass1")
	assert.Contains(t, dsn, "dbname=db1")
	assert.Contains(t, dsn, "sslmode=require")
	assert.Contains(t, dsn, "connect_timeout=5")
	// Format dasar: "host=... port=... user=... password=... dbname=... sslmode=... connect_timeout=..."
	expected := fmt.Sprintf("host=db-host port=5432 user=user1 password=pass1 dbname=db1 sslmode=require connect_timeout=5")
	assert.Equal(t, expected, dsn)
}

// ── SetupLogger ────────────────────────────────────────────────────────────────

func TestSetupLogger_json(t *testing.T) {
	defer func() { recover() }() // slog.SetDefault modifikasi global
	config.SetupLogger(config.LogConfig{Level: "info", Format: "json"})
}

func TestSetupLogger_text(t *testing.T) {
	defer func() { recover() }()
	config.SetupLogger(config.LogConfig{Level: "debug", Format: "text"})
}

func TestSetupLogger_warnLevel(t *testing.T) {
	defer func() { recover() }()
	config.SetupLogger(config.LogConfig{Level: "warn", Format: "json"})
}

func TestSetupLogger_errorLevel(t *testing.T) {
	defer func() { recover() }()
	config.SetupLogger(config.LogConfig{Level: "error", Format: "json"})
}

func TestSetupLogger_unknownLevelDefaultsInfo(t *testing.T) {
	defer func() { recover() }()
	config.SetupLogger(config.LogConfig{Level: "verbose", Format: "json"})
}

// ── Midtrans env ───────────────────────────────────────────────────────────────

func TestLoad_midtransDefaults(t *testing.T) {
	setRequired(t)

	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "sandbox", cfg.Midtrans.Env)
	assert.Equal(t, "", cfg.Midtrans.ServerKey) // opsional di lokal
}

func TestLoad_midtransProduction(t *testing.T) {
	setRequired(t)
	t.Setenv("MIDTRANS_ENV", "production")
	t.Setenv("MIDTRANS_SERVER_KEY", "server-key-prod")

	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "production", cfg.Midtrans.Env)
	assert.Equal(t, "server-key-prod", cfg.Midtrans.ServerKey)
}
