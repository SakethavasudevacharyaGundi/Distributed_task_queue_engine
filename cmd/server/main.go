package main

import (
	"fmt"
	"net/http"

	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/api"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/config"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/logger"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/queue"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/ratelimiter"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/storage"
	"go.uber.org/zap"
)

func main() {
	cfg := config.Load()

	repo, err := storage.NewPostgresStorage(
		cfg.PostgresDSN,
	)
	err = repo.Init()
	if err != nil {
		fmt.Println("Failed to initialize storage", zap.Error(err))
		panic(err)
	}
	q := queue.NewRedisQueue(
		cfg.RedisAddr,
		"",
		0,
		repo,
	)
	limiter := &ratelimiter.RateLimiter{
		Client: q.Client(),
	}
	logger.Init()
	defer logger.Log.Sync()

	httpServer := &api.HTTPServer{
		Queue:   q,
		Repo:    repo,
		Limiter: limiter,
	}

	http.HandleFunc(
		"/tasks",
		httpServer.SubmitTask,
	)

	http.HandleFunc("/", func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		w.Write([]byte("Task Queue Server Running"))
	})

	logger.Log.Info("HTTP server running on :8080")

	err = http.ListenAndServe(
		":8080",
		nil,
	)

	if err != nil {
		panic(err)
	}
}
