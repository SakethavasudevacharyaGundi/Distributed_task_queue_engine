package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/handlers"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/models"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/queue"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/metrics"
)

type Worker struct {
	ID    int
	Queue *queue.RedisQueue
}

func (w *Worker) Start() {
	fmt.Printf("Worker %d started\n", w.ID)
	for {
		task, err := w.Queue.Dequeue()
		if err != nil {
			fmt.Printf("Worker %d failed to dequeue task: %v\n", w.ID, err)
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
			fmt.Printf("Worker %d crashed while processing task: %s, error: %v\n", w.ID, task.ID, r)

		}
	}()
	fmt.Printf("worker %d processing task: %s\n", w.ID, task.ID)
	err := w.Queue.MarkProcessing(task)
	if err != nil {
		fmt.Printf("Worker %d failed to update task status: %v\n", w.ID, err)
		return
	}
	handler, ok := handlers.Registry[task.Name]
	if !ok {
		fmt.Printf("Worker %d no handler found for task: %s\n", w.ID, task.Name)
		return
	}
	start := time.Now()
	err = handler.Handle(context.Background(), task)
	if err != nil {
		fmt.Printf("Worker %d failed to process task: %s, error: %v\n", w.ID, task.ID, err)
		task.RetryCount++
		metrics.RetryCount.Inc()
		metrics.TasksFailed.Inc()
		w.Queue.Client().ZRem(context.Background(), "processing", task.ID)
		if task.RetryCount > task.MaxRetries {
			fmt.Printf("Task %s exceeded max retries\n", task.ID)
			err = w.Queue.MoveToDeadletter(task)
			if err != nil {
				fmt.Println(err)
			}
			return
		}
		delay := time.Duration(1<<task.RetryCount) * time.Second
		fmt.Printf("Worker %d will retry task: %s after %v\n", w.ID, task.ID, delay)
		err = w.Queue.AddtoRetryQueue(task, delay)
		if err != nil {
			fmt.Println(err)
		}
		return
	}
	task.Status = models.StatusCompleted
	fmt.Printf("worker %d completed task: %s\n", w.ID, task.ID)
	err = w.Queue.MarkCompleted(task)
	metrics.TasksProcessed.Inc()
	metrics.ProcessingTime.Observe(time.Since(start).Seconds())
	if err != nil {
		fmt.Println(err)
	}
}
