package api

import (
	"context"
	"time"
	"errors"
	"github.com/google/uuid"

	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/models"
	pb "github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/pb"
)
type TaskServer struct {
	pb.UnimplementedTaskServiceServer
}
func (s *TaskServer) SubmitTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	if req.Name == "" {
		return nil, errors.New("task name is required")
	}
	task := &models.Task{
		ID:         uuid.New().String(),
		Name:       req.Name,
		Payload:    req.Payload,
		Status:     models.StatusPending,
		Priority:   int(req.Priority),
		MaxRetries: int(req.MaxRetries),
		RetryCount: 0,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	// Save task to database (not implemented)
	return &pb.TaskResponse{Id: task.ID,
		Status: task.Status}, nil	
}


