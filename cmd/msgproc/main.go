package main

import (
	"context"
	"errors"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"log/slog"
	"msgproc/internal/config"
	"msgproc/internal/http-server/handlers/msg/process"
	"msgproc/internal/http-server/handlers/msg/stat"
	mvLog "msgproc/internal/http-server/middleware/logger"
	"msgproc/internal/lib/logger/sl"
	"msgproc/internal/services/kafka"
	"msgproc/internal/services/msgproc"
	"msgproc/internal/services/msgstat"
	"msgproc/internal/storage/postgres"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	envLocal = "local"
	envDev   = "dev"
	envProd  = "prod"
)

func main() {
	cfg := config.MustLoad()
	log := setupLogger(cfg.Env)
	log.Info("Starting msgproc...")

	storage, err := postgres.NewStorage(
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Postgres.Database,
		cfg.Postgres.Host,
	)
	if err != nil {
		log.Error("failed to create storage", sl.Err(err))
		os.Exit(1)
	}

	brokers := []string{"localhost:9092"}
	topic := "msgproc"

	sender, err := kafka.NewKafkaSender(brokers, topic, log)
	if err != nil {
		log.Error("failed to create Kafka sender", sl.Err(err))
		return
	}

	receiver, err := kafka.NewKafkaReceiver(log, brokers, topic, "msgproc")
	if err != nil {
		log.Error("failed to create Kafka receiver", sl.Err(err))
		return
	}

	done := make(chan struct{})

	ctx, cancel := context.WithTimeout(context.Background(), cfg.CtxTimeout)

	go func() {
		defer close(done)
		defer cancel()

		err := receiver.ProcessMessages(ctx, storage)
		if err != nil {
			log.Error("failed to process messages", sl.Err(err))
		}
	}()

	// Создаем HTTP сервер
	msgStatService := msgstat.New(log, storage)
	msgProc := msgproc.New(log, storage, sender)

	router := chi.NewRouter()
	router.Use(middleware.RequestID)
	router.Use(mvLog.New(log))
	router.Use(middleware.Recoverer)
	router.Use(middleware.URLFormat)

	router.Route("/api/v1", func(r chi.Router) {
		r.Post("/msg", process.New(log, msgProc))
		r.Get("/stat", stat.New(log, msgStatService))
	})

	log.Info("starting server", slog.String("address", cfg.HTTPServer.Host))

	srv := &http.Server{
		Addr:         cfg.HTTPServer.Host,
		Handler:      router,
		ReadTimeout:  cfg.HTTPServer.ReadTimeout,
		WriteTimeout: cfg.HTTPServer.WriteTimeout,
		IdleTimeout:  cfg.HTTPServer.IdleTimeout,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("failed to start server", sl.Err(err))
		}
	}()

	log.Info("server started")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	log.Info("stopping server")

	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Error("failed to stop server", sl.Err(err))
	}

	log.Info("server stopped")

	select {
	case <-done:
		log.Info("Kafka consumer gracefully stopped")
	case <-time.After(30 * time.Second):
		log.Error("Kafka consumer did not stop gracefully in time")
	}
}

func setupLogger(env string) *slog.Logger {
	var log *slog.Logger

	switch env {
	case envLocal:
		log = slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
		)
	case envDev:
		log = slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
		)
	case envProd:
		log = slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
		)
	}

	return log
}
