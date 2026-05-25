package storage

import (
	"database/sql"

	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/models"
	_ "github.com/lib/pq"
)

type PostgresStorage struct {
	DB *sql.DB
}

func NewPostgresStorage(connStr string) (*PostgresStorage, error) {
	connStr = `
	host=postgres
	port=5432
	user=postgres
	password=postgres
	dbname=taskdb
	sslmode=disable
	`

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	return &PostgresStorage{DB: db}, nil
}
func(p *PostgresStorage) SaveTask(task *models.Task) error {
	_, err := p.DB.Exec(`
	INSERT INTO tasks (id,type, payload,priority, status,retry_count,max_retries, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, task.ID, task.Name, task.Payload, task.Priority, task.Status, task.RetryCount, task.MaxRetries, task.CreatedAt, task.UpdatedAt)
	return err
}
func(p *PostgresStorage) UpdateTaskStatus(task *models.Task) error {
	_, err := p.DB.Exec(`
	UPDATE tasks SET status = $1,retry_count = $2, updated_at = $3 WHERE id = $4
	`, task.Status, task.RetryCount, task.UpdatedAt, task.ID)
	return err
}
func(p *PostgresStorage) GetTask(id string) (*models.Task, error) {
	var task models.Task
	err := p.DB.QueryRow(`
	SELECT id, type, payload, priority, status, retry_count, max_retries, created_at, updated_at
	FROM tasks WHERE id = $1
	`, id).Scan(&task.ID, &task.Name, &task.Payload, &task.Priority, &task.Status, &task.RetryCount, &task.MaxRetries, &task.CreatedAt, &task.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &task, nil
}
func(p *PostgresStorage) ListDeadTasks() ([]models.Task, error) {
	rows, err := p.DB.Query(`
	SELECT id, type, payload, priority, status, retry_count, max_retries, created_at, updated_at
	FROM tasks WHERE status = 'dead_letter'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []models.Task

	for rows.Next() {
		var task models.Task
		err := rows.Scan(&task.ID, &task.Name, &task.Payload, &task.Priority, &task.Status, &task.RetryCount, &task.MaxRetries, &task.CreatedAt, &task.UpdatedAt)
		if err != nil {
			continue
		}
		tasks=append(tasks, task)
	}
	return tasks, nil

}