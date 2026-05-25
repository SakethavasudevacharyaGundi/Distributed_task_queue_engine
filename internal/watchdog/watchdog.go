package watchdog

import (
	"context"
	"fmt"
	"time"

	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/models"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/queue"
	"github.com/go-redis/redis/v8"
)

type Watchdog struct {
	Queue *queue.RedisQueue
}

func (w *Watchdog) Start() {
	var ctx = context.Background()
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		now := time.Now().Unix()
		fmt.Printf("Watchdog checking for stale tasks at %d\n", now)
		staleBefore := now - 10
		fmt.Println("Now:", now)
		fmt.Println("Stale Before:", staleBefore)

		results, err := w.Queue.Client().ZRangeByScore(ctx, "processing", &redis.ZRangeBy{
			Min: "-inf",
			Max: fmt.Sprintf(
				"%d", staleBefore,
			),
		},
		).Result()
		fmt.Println(results)
		if err != nil {
			fmt.Println(err)
			continue
		}
		for _, taskID := range results {
			fmt.Printf("Requeuing Stale Task%s \n", taskID)
			task, err := w.Queue.GetTask(taskID)
			if err != nil {
				continue
			}
			if(task == nil) {
				continue
			}
			task.Status = models.StatusPending
			task.UpdatedAt = time.Now()
			err = w.Queue.UpdatedTask(task)
			if err != nil {
				continue
			}
			err = w.Queue.RequeueTask(task)
			if err != nil {
				continue
			}
			w.Queue.Client().ZRem(ctx, "processing", taskID)
		}
	}
}
