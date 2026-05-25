package queue
import (
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/models"
)
type Queue interface {
	Enqueue(task *models.Task) error
	Dequeue() (*models.Task, error)
}
