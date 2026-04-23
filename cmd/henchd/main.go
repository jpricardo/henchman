package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jpricardo/henchman/internal/budget"
	"github.com/jpricardo/henchman/internal/metrics"
	"github.com/jpricardo/henchman/internal/registry"
	"github.com/jpricardo/henchman/internal/server"
)

func main() {
	maxMemoryBytes := getMaxMemoryBytes(int64(512 * 1024 * 1024)) // 512MB default
	gb := budget.NewGlobalBudget(maxMemoryBytes)
	r := registry.NewRegistry(gb)
	grpcSrv := server.NewGRPCServer(r)

	port := getPort("6474")
	host := getHost("127.0.0.1")
	address := host + ":" + port

	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Panicf("unable to bind to address %s", address)
	}

	go grpcSrv.Serve(lis)

	metricsPort := getMetricsPort(9090)
	metricsSrv := metrics.NewServer(metricsPort, r)
	go func() {
		if err := metricsSrv.Start(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server error: %v", err)
		}
	}()

	log.Println("ready")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, os.Interrupt)
	<-quit

	grpcSrv.GracefulStop()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("metrics server shutdown error: %v", err)
	}
	r.Shutdown()
}

func getHost(fallback string) string {
	hostname, err := os.Hostname()
	if err != nil {
		return fallback
	}

	addrs, err := net.LookupHost(hostname)
	if err != nil || len(addrs) == 0 {
		return fallback
	}

	for _, addr := range addrs {
		ip := net.ParseIP(addr)
		if ip != nil && ip.To4() != nil && !ip.IsLoopback() {
			return addr
		}
	}

	return fallback
}

func getPort(fallback string) string {
	port := os.Getenv("HENCH_PORT")
	if port == "" {
		return fallback
	}

	return port
}

func getMetricsPort(fallback int) int {
	port := os.Getenv("HENCH_METRICS_PORT")
	if port == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(port)
	if err != nil {
		log.Printf("invalid HENCH_METRICS_PORT: %s, using fallback %d", port, fallback)
		return fallback
	}

	return parsed
}

func getMaxMemoryBytes(fallback int64) int64 {
	maxMemoryStr := os.Getenv("HENCH_MAX_MEMORY")

	if maxMemoryStr == "" {
		return fallback
	}

	parsed, err := strconv.ParseInt(maxMemoryStr, 10, 64)
	if err != nil {
		log.Printf("invalid HENCH_MAX_MEMORY: %s, using fallback %d", maxMemoryStr, fallback)
		return fallback
	}

	return parsed
}
