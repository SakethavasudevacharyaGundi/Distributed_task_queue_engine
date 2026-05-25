package watchdog

import (
	"context"
	"fmt"
	"time"

	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/logger"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/models"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/queue"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

type Watchdog struct {
	Queue *queue.RedisQueue
}

func (w *Watchdog) Start() {
	var ctx = context.Background()
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		now := time.Now().Unix()
		logger.Log.Info("Watchdog checking for stale tasks", zap.Int64("timestamp", now))
		staleBefore := now - 10
		logger.Log.Info("Stale before", zap.Int64("timestamp", staleBefore))

		results, err := w.Queue.Client().ZRangeByScore(ctx, "processing", &redis.ZRangeBy{
			Min: "-inf",
			Max: fmt.Sprintf(
				"%d", staleBefore,
			),
		},
		).Result()
		logger.Log.Info("Found stale tasks", zap.Strings("task_ids", results))
		if err != nil {
			logger.Log.Error("Watchdog failed to find stale tasks", zap.Error(err))
			continue
		}
		for _, taskID := range results {
			logger.Log.Info("Requeuing stale task", zap.String("task_id", taskID))
			task, err := w.Queue.GetTask(taskID)
			if err != nil {
				logger.Log.Error("Watchdog failed to get stale task", zap.String("task_id", taskID), zap.Error(err))
				continue
			}
			if task == nil {
				logger.Log.Warn("Stale task not found", zap.String("task_id", taskID))
				continue
			}
			task.Status = models.StatusPending
			task.UpdatedAt = time.Now()
			err = w.Queue.UpdatedTask(task)
			if err != nil {
				logger.Log.Error("Watchdog failed to update stale task", zap.String("task_id", taskID), zap.Error(err))
				continue
			}
			exists, err := w.Queue.Client().Exists(ctx, "task:"+taskID).Result()
			if err != nil {
				continue
			}
			if exists == 1 {
				logger.Log.Info("Task already exists in queue, skipping requeue", zap.String("task_id", taskID))
				continue
			}

			err = w.Queue.RequeueTask(task)
			if err != nil {
				logger.Log.Error("Watchdog failed to requeue stale task", zap.String("task_id", taskID), zap.Error(err))
				continue
			}
			w.Queue.Client().ZRem(ctx, "processing", taskID)
		}
	}
}
