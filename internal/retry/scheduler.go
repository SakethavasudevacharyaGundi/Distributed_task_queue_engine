package retry

import (
	"context"
	"fmt"
	"time"

	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/models"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/queue"
	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

type Scheduler struct {
	Queue *queue.RedisQueue
}

func (s *Scheduler) Start() {
	ticker := time.NewTicker(1 * time.Minute)
	for range ticker.C {
		now := time.Now().Unix()
		results, err := s.Queue.Client().ZRangeByScore(ctx, "retry_queue", &redis.ZRangeBy{
			Min: "0",
			Max: fmt.Sprintf("%d", now),
		}).Result()
		if err != nil {
			continue
		}
		for _, taskID := range results {
			task, err := s.Queue.GetTask(taskID)
			if err != nil {
				continue
			}
			fmt.Printf("Retrying task %s\n", task.ID)
			task.Status = models.StatusPending
			task.UpdatedAt = time.Now()
			err = s.Queue.UpdatedTask(task)

			if err != nil {
				continue
			}
			err = s.Queue.RequeueTask(task)
			if err != nil {
				continue
			}
			s.Queue.Client().ZRem(ctx, "retry_queue", task.ID)
		}
	}
}
