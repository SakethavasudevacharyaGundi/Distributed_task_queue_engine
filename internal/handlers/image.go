package handlers
import(
	"context"
	"fmt"
	"time"
	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/models"
)
type ProcessImageHandler struct {}

func(h*ProcessImageHandler) Handle(ctx context.Context, task *models.Task) error {
	fmt.Printf("Processing image for task: %s\n", task.ID)
	time.Sleep(3 * time.Second)
	fmt.Printf("Image processed for task: %s\n", task.ID)
	return nil
}