package job

import (
	"encoding/json"
	"time"
)

type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusRetrying  Status = "retrying"
)

type Job struct {
	ID             string          `json:"id"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Status         Status          `json:"status"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	LastError      *string         `json:"last_error,omitempty"`
	NextAttemptAt  *time.Time      `json:"next_attempt_at,omitempty"`
}

type CreateRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	MaxAttempts    int             `json:"max_attempts"`
	TimeoutSeconds int             `json:"timeout_seconds"`
}
