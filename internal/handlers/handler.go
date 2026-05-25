package handlers

import (
	"context"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/models"
)

type TaskHandler interface {
	Handle(
		ctx context.Context,
		task *models.Task,
	) error
}
