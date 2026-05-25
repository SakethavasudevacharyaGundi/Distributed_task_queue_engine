package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/models"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/queue"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/ratelimiter"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/storage"
)

type HTTPServer struct {
	Queue   *queue.RedisQueue
	Repo    *storage.PostgresStorage
	Limiter *ratelimiter.RateLimiter
}

type SubmitTaskRequest struct {
	Name       string `json:"name"`
	Payload    string `json:"payload"`
	Priority   int    `json:"priority"`
	MaxRetries int    `json:"max_retries"`
	TenantID   string `json:"tenant_id"`
}

func (s *HTTPServer) SubmitTask(
	w http.ResponseWriter,
	r *http.Request,
) {

	if r.Method != http.MethodPost {
		http.Error(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)
		return
	}

	var req SubmitTaskRequest
	allowed, err := s.Limiter.Allow("ratelimit:"+r.RemoteAddr, 100, time.Minute)
	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusInternalServerError,
		)
		return
	}
	if !allowed {
		http.Error(
			w,
			"rate limit exceeded",
			http.StatusTooManyRequests,
		)
		return
	}
	err = json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusBadRequest,
		)
		return
	}

	task := &models.Task{
		ID:         uuid.New().String(),
		Name:       req.Name,
		Payload:    req.Payload,
		Status:     models.StatusPending,
		Priority:   req.Priority,
		MaxRetries: req.MaxRetries,
		RetryCount: 0,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
		Claimed:    false,
		TenantID:   req.TenantID,
	}
	err = s.Repo.CreateTask(task)
	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusInternalServerError,
		)
		return
	}
	err = s.Queue.Enqueue(task)
	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusInternalServerError,
		)
		return
	}

	w.WriteHeader(http.StatusCreated)

	json.NewEncoder(w).Encode(task)
}
