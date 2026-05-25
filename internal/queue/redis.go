package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/metrics"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/models"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/storage"
	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

type RedisQueue struct {
	client *redis.Client
	repo   *storage.PostgresStorage
}

func NewRedisQueue(addr, password string, db int, repo *storage.PostgresStorage) *RedisQueue {
	rdb := redis.NewClient(&redis.Options{
		Addr: "redis:6379",
	})
	return &RedisQueue{client: rdb, repo: repo}
}

func (r *RedisQueue) Ping() error {
	_, err := r.client.Ping(ctx).Result()
	return err
}

func (r *RedisQueue) Enqueue(task *models.Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}
	taskKey := "task:" + task.ID
	err = r.client.Set(ctx, taskKey, data, 0).Err()
	if err != nil {
		return err
	}
	queueName := "queue_low"
	switch task.Priority {
	case models.PriorityMedium:
		queueName = "queue_medium"
	case models.PriorityHigh:
		queueName = "queue_high"
	}
	err = r.client.ZAdd(
		ctx,
		queueName,
		&redis.Z{
			Score:  float64(time.Now().Unix()),
			Member: task.ID,
		},
	).Err()
	if err != nil {
		return err
	}
	return nil
}
func (r *RedisQueue) Dequeue() (*models.Task, error) {
	queues := []string{"queue_high", "queue_medium", "queue_low"}
	for _, queueName := range queues {
		result, err := r.client.ZPopMin(ctx, queueName).Result()
		if err != nil {
			return nil, err
		}
		if len(result) == 0 {
			continue
		}
		taskID := result[0].Member.(string)
		taskData, err := r.client.Get(ctx, "task:"+taskID).Result()
		if err != nil {
			return nil, err
		}
		var task models.Task
		err = json.Unmarshal([]byte(taskData), &task)
		if err != nil {
			return nil, err
		}
		return &task, nil
	}

	return nil, nil
}
func (r *RedisQueue) UpdatedTask(task *models.Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}

	return r.client.Set(ctx, "task:"+task.ID, data, 0).Err()
}
func (r *RedisQueue) MarkProcessing(
	task *models.Task,
) error {

	task.Status = models.StatusInProgress
	task.UpdatedAt = time.Now()

	err := r.UpdatedTask(task)

	if err != nil {
		return err
	}

	err = r.client.ZAdd(
		ctx,
		"processing",
		&redis.Z{
			Score:  float64(time.Now().Unix()),
			Member: task.ID,
		},
	).Err()

	if err != nil {
		return err
	}
	err = r.repo.UpdateTaskStatus(task)

	if err != nil {
		fmt.Println(
			"POSTGRES UPDATE FAILED:",
			err,
		)

		// TEMPORARY:
		// don't abort processing
	}
	task.UpdatedAt = time.Now()

	fmt.Printf(
		"Task %s added to processing set\n",
		task.ID,
	)

	return nil
}
func (r *RedisQueue) MarkCompleted(task *models.Task) error {
	task.Status = models.StatusCompleted

	task.UpdatedAt = time.Now()
	err := r.UpdatedTask(task)
	if err != nil {
		return err
	}
	err = r.repo.UpdateTaskStatus(task)
	if err != nil {
		return err
	}
	return r.client.ZRem(ctx, "queue_processing", task.ID).Err()
}
func (r *RedisQueue) GetTask(taskID string) (*models.Task, error) {
	data, err := r.client.Get(ctx, "task:"+taskID).Result()
	if err != nil {
		return nil, err
	}
	var task models.Task
	err = json.Unmarshal([]byte(data), &task)
	if err != nil {
		return nil, err
	}
	return &task, nil
}
func (r *RedisQueue) RequeueTask(task *models.Task) error {
	queueName := "queue_low"
	switch task.Priority {
	case models.PriorityMedium:
		queueName = "queue_medium"
	case models.PriorityHigh:
		queueName = "queue_high"
	}
	err := r.client.ZAdd(
		ctx,
		queueName,
		&redis.Z{
			Score:  float64(time.Now().Unix()),
			Member: task.ID,
		},
	).Err()
	if err != nil {
		return err
	}
	return nil

}
func (r *RedisQueue) Client() *redis.Client {
	return r.client
}
func (r *RedisQueue) AddtoRetryQueue(task *models.Task, delay time.Duration) error {
	retryAt := time.Now().Add(delay).Unix()
	task.Status = models.StatusRetrying
	task.UpdatedAt = time.Now()
	err := r.UpdatedTask(task)

	if err != nil {
		return err
	}
	err = r.repo.UpdateTaskStatus(task)

	if err != nil {
		return err
	}

	return r.client.ZAdd(
		ctx,
		"retry_queue",
		&redis.Z{
			Score:  float64(retryAt),
			Member: task.ID,
		},
	).Err()

}
func (r *RedisQueue) MoveToDeadletter(task *models.Task) error {
	task.Status = models.StatusDeadLetter
	task.UpdatedAt = time.Now()
	err := r.UpdatedTask(task)
	if err != nil {
		return err
	}
	err = r.repo.UpdateTaskStatus(task)

	if err != nil {
		return err
	}
	err = r.client.ZAdd(
		ctx,
		"deadletter_queue",
		&redis.Z{
			Score:  float64(time.Now().Unix()),
			Member: task.ID,
		},
	).Err()
	if err != nil {
		return err
	}
	metrics.DLQsize.Inc()
	return nil
}
func (r *RedisQueue) UpdateQueueDepthMetric() {

	high, _ := r.client.ZCard(
		ctx,
		"queue_high",
	).Result()

	medium, _ := r.client.ZCard(
		ctx,
		"queue_medium",
	).Result()

	low, _ := r.client.ZCard(
		ctx,
		"queue_low",
	).Result()

	total := high + medium + low

	metrics.QueueDepth.Set(
		float64(total),
	)
}
