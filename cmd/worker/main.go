package main

import (
	"net/http"

	"time"

	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/metrics"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/queue"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/retry"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/storage"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/watchdog"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/worker"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	repo, err := storage.NewPostgresStorage(
		"host=postgres port=5432 user=postgres password=postgres dbname=taskdb sslmode=disable")
	if err != nil {
		panic(err)
	}
	q := queue.NewRedisQueue("redis:6379", "", 0, repo)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for range ticker.C {
			q.UpdateQueueDepthMetric()
		}
	}()
	workerCount := 1
	s := &retry.Scheduler{
		Queue: q,
	}
	go s.Start()
	for i := 0; i < workerCount; i++ {
		w := &worker.Worker{
			ID:    i + 1,
			Queue: q,
		}
		go w.Start()
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
	select {}
}
