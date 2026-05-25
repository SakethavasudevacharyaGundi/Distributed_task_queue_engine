package main

import (
	"fmt"
	"net/http"

	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/config"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/logger"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/metrics"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/queue"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/retry"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/storage"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/watchdog"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/worker"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	cfg := config.Load()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	logger.Init()
	defer logger.Log.Sync()
	go func() {
		<-sigChan
		fmt.Println("Received shutdown signal, stopping workers...")
		cancel()
	}()
	repo, err := storage.NewPostgresStorage(
		cfg.PostgresDSN)
	err = repo.Init()
	if err != nil {
		panic(err)
	}
	q := queue.NewRedisQueue(cfg.RedisAddr, "", 0, repo)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for range ticker.C {
			q.UpdateQueueDepthMetric()
		}
	}()
	WorkerCount := cfg.WorkerCount
	s := &retry.Scheduler{
		Queue: q,
	}
	go s.Start()
	for i := 0; i < WorkerCount; i++ {
		w := &worker.Worker{
			ID:    i + 1,
			Queue: q,
		}
		go w.Start(ctx)
	}
	wd := &watchdog.Watchdog{
		Queue: q,
	}
	go wd.Start()
	metrics.Init()
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":2112", nil)
	}()
	<-ctx.Done()
	fmt.Println("Shutting down gracefully...")

}
