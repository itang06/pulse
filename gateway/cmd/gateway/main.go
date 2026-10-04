// Pulse ingestion gateway.
//
// The gateway accepts ExportBatch requests, publishes validated events to
// Kafka, and serves Prometheus metrics.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	pulsev1 "github.com/itang06/pulse/gateway/gen/pulse/v1"
	"github.com/itang06/pulse/gateway/internal/kafka"
	"github.com/itang06/pulse/gateway/internal/metrics"
	"github.com/itang06/pulse/gateway/internal/server"
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	grpcAddr := envOr("PULSE_GRPC_ADDR", ":50051")
	metricsAddr := envOr("PULSE_METRICS_ADDR", ":9464")
	brokers := strings.Split(envOr("PULSE_KAFKA_BROKERS", "localhost:9092"), ",")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Kafka producer: connect and verify the cluster is reachable up front so a
	// misconfigured broker address fails at startup, not on first publish.
	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		log.Error("creating kafka producer", "err", err)
		os.Exit(1)
	}
	defer producer.Close()

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := producer.Ping(pingCtx); err != nil {
		log.Error("kafka unreachable", "brokers", brokers, "err", err)
		os.Exit(1)
	}
	log.Info("connected to kafka", "brokers", brokers)

	reg := metrics.NewRegistry()

	grpcServer := grpc.NewServer()
	pulsev1.RegisterTelemetryServiceServer(grpcServer, server.New(log, producer, reg))

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Error("listening", "addr", grpcAddr, "err", err)
		os.Exit(1)
	}

	// /metrics on a separate listener so Prometheus never competes with, or is
	// exposed on, the public ingest port.
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: promhttp.HandlerFor(reg.Registry, promhttp.HandlerOpts{})}

	errCh := make(chan error, 2)
	go func() {
		log.Info("gRPC server listening", "addr", grpcAddr)
		errCh <- grpcServer.Serve(lis)
	}()
	go func() {
		log.Info("metrics server listening", "addr", metricsAddr)
		if err := metricsSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err := <-errCh:
		log.Error("server failed", "err", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
	grpcServer.GracefulStop()
}
