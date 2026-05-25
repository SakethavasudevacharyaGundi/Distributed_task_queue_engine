package handlers

import (
	"context"
	"fmt"
	"errors"

	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/models"
)

type SendEmailHandler struct{}

func (h *SendEmailHandler) Handle(ctx context.Context, task *models.Task) error {
	fmt.Printf("Sending email for task: %s\n", task.ID)
	fmt.Printf("Email sent for task: %s\n", task.ID)
	return errors.New("failed to send email")
}
