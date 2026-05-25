package worker

import (
	"context"
	"time"

	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/handlers"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/logger"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/metrics"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/models"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/queue"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/storage"
	"go.uber.org/zap"
)

type Worker struct {
	ID    int
	Queue *queue.RedisQueue
	repo  *storage.PostgresStorage
}

func (w *Worker) Start(ctx context.Context) {
	logger.Log.Info("Worker started", zap.Int("id", w.ID))
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Worker stopping gracefully", zap.Int("id", w.ID))
			return
		default:
		}
		task, err := w.Queue.Dequeue()
		if err != nil {
			logger.Log.Error("Worker failed to dequeue task", zap.Int("id", w.ID), zap.Error(err))
			time.Sleep(time.Second)
			continue
		}
		if task == nil {
			time.Sleep(time.Second)
			continue
		}
		w.ProcessTask(task)
	}
}
func (w *Worker) ProcessTask(task *models.Task) {
	defer func() {
		if r := recover(); r != nil {
			logger.Log.Error("Worker crashed while processing task", zap.Int("id", w.ID), zap.String("task_id", task.ID), zap.Any("panic", r))

		}
	}()
	logger.Log.Info("worker processing task", zap.Int("id", w.ID), zap.String("task_id", task.ID))
	lockkey := "lock:" + task.ID
	acquired, err := w.Queue.Client().SetNX(context.Background(), lockkey, "1", 30*time.Second).Result()
	if err != nil {
		logger.Log.Error("Worker failed to acquire lock for task", zap.Int("id", w.ID), zap.String("task_id", task.ID), zap.Error(err))
		return
	}
	if !acquired {
		logger.Log.Info("Worker failed to acquire lock for task", zap.Int("id", w.ID), zap.String("task_id", task.ID))
		return
	}
	defer w.Queue.Client().Del(context.Background(), lockkey)
	claimed, err := w.Queue.ClaimTask(task.ID)
	if err != nil {
		logger.Log.Error("Worker failed to claim task", zap.Int("id", w.ID), zap.String("task_id", task.ID), zap.Error(err))
		return
	}
	if !claimed {
		logger.Log.Warn("task already claimed", zap.String("task_id", task.ID))
		return
	}
	err = w.Queue.MarkProcessing(task)
	if err != nil {
		logger.Log.Error("Worker failed to update task status", zap.Int("id", w.ID), zap.Error(err))
		return
	}
	handler, ok := handlers.Registry[task.Name]
	if !ok {
		logger.Log.Warn("Worker no handler found for task", zap.Int("id", w.ID), zap.String("task_name", task.Name))
		return
	}
	start := time.Now()
	err = handler.Handle(context.Background(), task)
	if err != nil {
		logger.Log.Error("Worker failed to process task", zap.Int("id", w.ID), zap.String("task_id", task.ID), zap.Error(err))
		task.RetryCount++
		metrics.RetryCount.Inc()
		metrics.TasksFailed.Inc()
		err = w.repo.ResetClaim(task.ID)
		if err != nil {
			logger.Log.Error("Worker failed to reset claim for task", zap.Int("id", w.ID), zap.String("task_id", task.ID), zap.Error(err))
		}
		w.Queue.Client().ZRem(context.Background(), "processing", task.ID)
		if task.RetryCount > task.MaxRetries {
			logger.Log.Info("Task exceeded max retries", zap.String("task_id", task.ID))
			err = w.Queue.MoveToDeadletter(task)
			if err != nil {
				logger.Log.Error("Worker failed to move task to deadletter", zap.Int("id", w.ID), zap.String("task_id", task.ID), zap.Error(err))
			}
			return
		}
		delay := time.Duration(1<<task.RetryCount) * time.Second
		logger.Log.Info("Worker will retry task", zap.Int("id", w.ID), zap.String("task_id", task.ID), zap.Duration("delay", delay))
		err = w.Queue.AddtoRetryQueue(task, delay)
		if err != nil {
			logger.Log.Error("Worker failed to add task to retry queue", zap.Int("id", w.ID), zap.String("task_id", task.ID), zap.Error(err))
		}
		return
	}
	task.Status = models.StatusCompleted
	logger.Log.Info("Worker completed task", zap.Int("id", w.ID), zap.String("task_id", task.ID))
	err = w.Queue.MarkCompleted(task)
	metrics.TasksProcessed.Inc()
	metrics.ProcessingTime.Observe(time.Since(start).Seconds())
	if err != nil {
		logger.Log.Error("Worker failed to mark task as completed", zap.Int("id", w.ID), zap.String("task_id", task.ID), zap.Error(err))
	}
}
