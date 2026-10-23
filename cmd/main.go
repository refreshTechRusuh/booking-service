package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/refreshTechRusuh/booking-service/internal/config"
	"github.com/refreshTechRusuh/booking-service/internal/delivery"
	pkgdb "github.com/refreshTechRusuh/booking-service/pkg/db"
	"github.com/refreshTechRusuh/booking-service/pkg/kafka"
	"github.com/refreshTechRusuh/booking-service/pkg/middleware"
	pkgvalkey "github.com/refreshTechRusuh/booking-service/pkg/valkey"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}
	config.SetupLogger(cfg.Log)

	rootCtx, cancelRoot := context.WithCancel(context.Background())
	defer cancelRoot()

	pool, err := pkgdb.NewPool(rootCtx, cfg.DB)
	if err != nil {
		slog.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}

	valkeyClient, err := pkgvalkey.NewClient(cfg.Valkey)
	if err != nil {
		slog.Error("failed to connect to valkey", "err", err)
		os.Exit(1)
	}

	producer := kafka.NewProducer(cfg.Kafka, slog.Default())

	app, err := delivery.New(rootCtx, cfg, pool, valkeyClient, producer)
	if err != nil {
		slog.Error("failed to init server", "err", err)
		os.Exit(1)
	}
	defer app.Close()

	// Start TTL sweeper goroutine
	app.Usecase.StartSweeper(rootCtx)

	// app.Handler sudah di-wrap RequestLogger (dari delivery.New).
	// OptionalAuth di-chain agar userId tersedia di semua route non-protected.
	handler := middleware.Chain(
		app.Handler,
		middleware.OptionalAuth(cfg.JWT.AccessTokenSecret),
	)

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.App.Port),
		ReadHeaderTimeout: 5 * time.Second,
		Handler:           handler,
	}

	go func() {
		slog.Info("booking-service listening", "port", cfg.App.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down...")
	cancelRoot()
	shutCtx, cancelShut := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShut()
	if err := srv.Shutdown(shutCtx); err != nil {
		slog.Error("shutdown error", "err", err)
	}
	slog.Info("booking-service stopped")
}
