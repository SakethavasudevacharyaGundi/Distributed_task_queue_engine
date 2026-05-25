package models

import "time"

type Task struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Payload    string    `json:"payload"`
	Status     string    `json:"status"`
	Priority   int       `json:"priority"`
	MaxRetries int       `json:"max_retries"`
	RetryCount int       `json:"retry_count"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	Claimed    bool      `json:"claimed"`
	TenantID   string    `json:"tenant_id"`
}

const (
	PriortyLow     = 1
	PriorityMedium = 2
	PriorityHigh   = 3
)
const (
	StatusPending    = "Pending"
	StatusInProgress = "InProgress"
	StatusCompleted  = "Completed"
	StatusFailed     = "Failed"
	StatusRetrying   = "Retrying"
	StatusDeadLetter = "DeadLetter"
)
