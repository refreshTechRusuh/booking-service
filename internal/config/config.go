package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/joho/godotenv"
	"github.com/refreshTechRusuh/booking-service/pkg/logctx"
)

// Config adalah konfigurasi seluruh aplikasi dibaca dari env.
type Config struct {
	App      AppConfig
	Log      LogConfig
	DB       DBConfig
	Valkey   ValkeyConfig
	Kafka    KafkaConfig
	JWT      JWTConfig
	Dukcapil DukcapilConfig
	EventSvc EventServiceConfig
	Midtrans MidtransConfig
	Email    EmailConfig
	SSE      SSEConfig
}

type AppConfig struct {
	Env  string
	Port int
}

type LogConfig struct {
	Level  string
	Format string
}

type DBConfig struct {
	Host            string
	Port            int
	User            string
	Password        string
	Name            string
	SSLMode         string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnTimeoutSecs int
}

type ValkeyConfig struct {
	Addr     string
	Password string
	TLS      bool
}

type KafkaConfig struct {
	Brokers  string
	Username string
	Password string
	TLS      bool
}

type JWTConfig struct {
	AccessTokenSecret    string
	PaymentTokenSecret   string
}

type DukcapilConfig struct {
	BaseURL    string
	TimeoutMs  int
	FailOpen   bool
}

type EventServiceConfig struct {
	BaseURL string
}

type MidtransConfig struct {
	ServerKey string
	Env       string // "sandbox" | "production"
}

type EmailConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	From     string
}

type SSEConfig struct {
	HeartbeatSeconds int
}

// Load membaca env (via godotenv) dan mengembalikan Config.
func Load() (*Config, error) {
	_ = godotenv.Load() // .env opsional (Cloud Run inject via secrets)

	cfg := &Config{
		App: AppConfig{
			Env:  getEnv("APP_ENV", "local"),
			Port: getEnvInt("PORT", getEnvInt("APP_PORT", 8081)),
		},
		Log: LogConfig{
			Level:  getEnv("LOG_LEVEL", "info"),
			Format: getEnv("LOG_FORMAT", "json"),
		},
		DB: DBConfig{
			Host:            getEnv("DB_HOST", "localhost"),
			Port:            getEnvInt("DB_PORT", 5432),
			User:            getEnv("DB_USER", "postgres"),
			Password:        getEnv("DB_PASSWORD", ""),
			Name:            getEnv("DB_NAME", "rusuh_booking"),
			SSLMode:         getEnv("DB_SSL_MODE", "disable"),
			MaxOpenConns:    getEnvInt("DB_MAX_OPEN_CONNS", 25),
			MaxIdleConns:    getEnvInt("DB_MAX_IDLE_CONNS", 10),
			ConnTimeoutSecs: getEnvInt("DB_CONN_TIMEOUT_SECONDS", 5),
		},
		Valkey: ValkeyConfig{
			Addr:     getEnv("VALKEY_ADDR", "localhost:6379"),
			Password: getEnv("VALKEY_PASSWORD", ""),
			TLS:      getEnvBool("VALKEY_TLS", false),
		},
		Kafka: KafkaConfig{
			Brokers:  getEnv("KAFKA_BROKERS", "localhost:9092"),
			Username: getEnv("KAFKA_USERNAME", ""),
			Password: getEnv("KAFKA_PASSWORD", ""),
			TLS:      getEnvBool("KAFKA_TLS", false),
		},
		JWT: JWTConfig{
			AccessTokenSecret:  mustEnv("ACCESS_TOKEN_SECRET"),
			PaymentTokenSecret: mustEnv("PAYMENT_TOKEN_SECRET"),
		},
		Dukcapil: DukcapilConfig{
			BaseURL:   getEnv("DUKCAPIL_BASE_URL", "http://localhost:8085"),
			TimeoutMs: getEnvInt("DUKCAPIL_TIMEOUT_MS", 3000),
			FailOpen:  getEnvBool("DUKCAPIL_FAIL_OPEN", true),
		},
		EventSvc: EventServiceConfig{
			BaseURL: getEnv("EVENT_SERVICE_URL", "http://localhost:8082"),
		},
		Midtrans: MidtransConfig{
			ServerKey: getEnv("MIDTRANS_SERVER_KEY", ""),   // opsional di lokal
			Env:       getEnv("MIDTRANS_ENV", "sandbox"),
		},
		// Email tidak dipakai booking-service (hanya worker-service yang kirim email)
		SSE: SSEConfig{
			HeartbeatSeconds: getEnvInt("SSE_HEARTBEAT_SECONDS", 30),
		},
	}

	// Produksi: kedua secret tidak boleh sama agar token antrian tidak bisa dipakai untuk bayar.
	if cfg.App.Env == "production" && cfg.JWT.AccessTokenSecret == cfg.JWT.PaymentTokenSecret {
		slog.Error("ACCESS_TOKEN_SECRET dan PAYMENT_TOKEN_SECRET tidak boleh sama di production")
		os.Exit(1)
	}

	return cfg, nil
}

// SetupLogger mengkonfigurasi slog global berdasarkan config.
func SetupLogger(cfg LogConfig) {
	level := slog.LevelInfo
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	var handler slog.Handler
	if cfg.Format == "text" {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	} else {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	}
	slog.SetDefault(slog.New(logctx.NewContextHandler(handler)))
}

// DSN mengembalikan PostgreSQL connection string untuk pgx.
func (d DBConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=%d",
		d.Host, d.Port, d.User, d.Password, d.Name, d.SSLMode, d.ConnTimeoutSecs,
	)
}

// ---- env helpers -------------------------------------------------------

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required env not set", slog.String("key", key))
		os.Exit(1)
	}
	return v
}
