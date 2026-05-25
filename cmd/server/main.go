package main

import (
	"fmt"
	"net/http"

	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/api"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/queue"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/storage"
)

func main() {

	repo, err := storage.NewPostgresStorage(
		"host=postgres port=5432 user=postgres password=postgres dbname=taskdb sslmode=disable",
	)

	if err != nil {
		panic(err)
	}

	q := queue.NewRedisQueue(
		"redis:6379",
		"",
		0,
		repo,
	)

	httpServer := &api.HTTPServer{
		Queue: q,
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

	fmt.Println("HTTP server running on :8080")

	err = http.ListenAndServe(
		":8080",
		nil,
	)

	if err != nil {
		panic(err)
	}
}
