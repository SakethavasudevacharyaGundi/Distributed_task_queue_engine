package storage

import (
	"database/sql"

	"github.com/Sakethavasudevacharyagundi/Distributed-task-queue-engine/internal/models"
	_ "github.com/lib/pq"
)

type PostgresStorage struct {
	DB *sql.DB
}

func (p *PostgresStorage) Init() error {
	query := `
	CREATE TABLE IF NOT EXISTS tasks (
		id TEXT PRIMARY KEY,
		type TEXT,
		payload JSONB,
		status TEXT,
		priority INT,
		retry_count INT,
		max_retries INT,
		created_at TIMESTAMPTZ,
		updated_at TIMESTAMPTZ,
		claimed BOOLEAN DEFAULT FALSE
		tenant_id TEXT
	)`
	_, err := p.DB.Exec(query)
	return err
}
func (p *PostgresStorage) CreateTask(task *models.Task) error {
	query := `
	INSERT INTO tasks (id,type, payload,priority, status,retry_count,max_retries, created_at, updated_at, claimed, tenant_id)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
	_, err := p.DB.Exec(
		query,
		task.ID,
		task.Name,
		task.Payload,
		task.Priority,
		task.Status,
		task.RetryCount,
		task.MaxRetries,
		task.CreatedAt,
		task.UpdatedAt,
		false,
	)
	return err
}

func NewPostgresStorage(connStr string) (*PostgresStorage, error) {

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	return &PostgresStorage{DB: db}, nil
}
func (p *PostgresStorage) SaveTask(task *models.Task) error {
	_, err := p.DB.Exec(`
	INSERT INTO tasks (id,type, payload,priority, status,retry_count,max_retries, created_at, updated_at, claimed, tenant_id)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`, task.ID, task.Name, task.Payload, task.Priority, task.Status, task.RetryCount, task.MaxRetries, task.CreatedAt, task.UpdatedAt, false, task.TenantID)
	return err
}
func (p *PostgresStorage) UpdateTaskStatus(task *models.Task) error {
	_, err := p.DB.Exec(`
	UPDATE tasks SET status = $1,retry_count = $2, updated_at = $3 WHERE id = $4
	`, task.Status, task.RetryCount, task.UpdatedAt, task.ID)
	return err
}
func (p *PostgresStorage) GetTask(id string) (*models.Task, error) {
	var task models.Task
	err := p.DB.QueryRow(`
	SELECT id, type, payload, priority, status, retry_count, max_retries, created_at, updated_at, claimed, tenant_id
	FROM tasks WHERE id = $1
	`, id).Scan(&task.ID, &task.Name, &task.Payload, &task.Priority, &task.Status, &task.RetryCount, &task.MaxRetries, &task.CreatedAt, &task.UpdatedAt, &task.Claimed, &task.TenantID)
	if err != nil {
		return nil, err
	}
	return &task, nil
}
func (p *PostgresStorage) ListDeadTasks() ([]models.Task, error) {
	rows, err := p.DB.Query(`
	SELECT id, type, payload, priority, status, retry_count, max_retries, created_at, updated_at, claimed, tenant_id
	FROM tasks WHERE status = 'dead_letter'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []models.Task

	for rows.Next() {
		var task models.Task
		err := rows.Scan(&task.ID, &task.Name, &task.Payload, &task.Priority, &task.Status, &task.RetryCount, &task.MaxRetries, &task.CreatedAt, &task.UpdatedAt, &task.TenantID)
		if err != nil {
			continue
		}
		tasks = append(tasks, task)
	}
	return tasks, nil

}
func (p *PostgresStorage) ClaimTask(taskID string) (bool, error) {
	query := `
	UPDATE tasks
	SET claimed = true
	WHERE id = $1
	AND claimed = false
`
	result, err := p.DB.Exec(query, taskID)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows == 1, nil
}
func (p *PostgresStorage) ResetClaim(
	taskID string,
) error {

	query := `
	UPDATE tasks
	SET claimed = false
	WHERE id = $1
	`

	_, err := p.DB.Exec(
		query,
		taskID,
	)

	return err
}
