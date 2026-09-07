package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sindhoora-17/taskforge/internal/job"
)

var ErrJobNotFound = errors.New("job not found")

type PostgresJobRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresJobRepository(pool *pgxpool.Pool) *PostgresJobRepository {
	return &PostgresJobRepository{
		pool: pool,
	}
}

func (r *PostgresJobRepository) Create(
	ctx context.Context,
	newJob job.Job,
) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin job creation transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	createJobQuery := `
		INSERT INTO jobs (
			id,
			type,
			payload,
			status,
			attempts,
			max_attempts,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	_, err = tx.Exec(
		ctx,
		createJobQuery,
		newJob.ID,
		newJob.Type,
		newJob.Payload,
		string(newJob.Status),
		newJob.Attempts,
		newJob.MaxAttempts,
		newJob.CreatedAt,
		newJob.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}

	createOutboxQuery := `
		INSERT INTO job_outbox (job_id)
		VALUES ($1)
	`

	if _, err := tx.Exec(
		ctx,
		createOutboxQuery,
		newJob.ID,
	); err != nil {
		return fmt.Errorf("insert job outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit job creation transaction: %w", err)
	}

	return nil
}

func (r *PostgresJobRepository) GetByID(
	ctx context.Context,
	jobID string,
) (job.Job, error) {
	query := `
		SELECT
			id::text,
			type,
			payload,
			status,
			attempts,
			max_attempts,
			created_at,
			updated_at,
			last_error,
			next_attempt_at
		FROM jobs
		WHERE id = $1
	`

	var storedJob job.Job
	var status string

	err := r.pool.QueryRow(ctx, query, jobID).Scan(
		&storedJob.ID,
		&storedJob.Type,
		&storedJob.Payload,
		&status,
		&storedJob.Attempts,
		&storedJob.MaxAttempts,
		&storedJob.CreatedAt,
		&storedJob.UpdatedAt,
		&storedJob.LastError,
		&storedJob.NextAttemptAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, ErrJobNotFound
	}

	if err != nil {
		return job.Job{}, err
	}

	storedJob.Status = job.Status(status)

	return storedJob, nil
}

func (r *PostgresJobRepository) UpdateStatus(
	ctx context.Context,
	jobID string,
	status job.Status,
	attempts int,
) error {
	query := `
		UPDATE jobs
		SET
			status = $2,
			attempts = $3,
			last_error = CASE
				WHEN $2 = 'completed' THEN NULL
				ELSE last_error
			END,
			next_attempt_at = CASE
				WHEN $2 IN ('running', 'completed', 'failed') THEN NULL
				ELSE next_attempt_at
			END,
			updated_at = NOW()
		WHERE id = $1
	`

	result, err := r.pool.Exec(
		ctx,
		query,
		jobID,
		string(status),
		attempts,
	)
	if err != nil {
		return err
	}

	if result.RowsAffected() == 0 {
		return ErrJobNotFound
	}

	return nil
}

func (r *PostgresJobRepository) MarkRetrying(
	ctx context.Context,
	jobID string,
	attempts int,
	lastError string,
	nextAttemptAt time.Time,
) error {
	query := `
		UPDATE jobs
		SET
			status = 'retrying',
			attempts = $2,
			last_error = $3,
			next_attempt_at = $4,
			updated_at = NOW()
		WHERE id = $1
	`

	result, err := r.pool.Exec(
		ctx,
		query,
		jobID,
		attempts,
		lastError,
		nextAttemptAt,
	)
	if err != nil {
		return err
	}

	if result.RowsAffected() == 0 {
		return ErrJobNotFound
	}

	return nil
}

func (r *PostgresJobRepository) MarkFailed(
	ctx context.Context,
	jobID string,
	attempts int,
	lastError string,
) error {
	query := `
		UPDATE jobs
		SET
			status = 'failed',
			attempts = $2,
			last_error = $3,
			next_attempt_at = NULL,
			updated_at = NOW()
		WHERE id = $1
	`

	result, err := r.pool.Exec(
		ctx,
		query,
		jobID,
		attempts,
		lastError,
	)
	if err != nil {
		return err
	}

	if result.RowsAffected() == 0 {
		return ErrJobNotFound
	}

	return nil
}

func (r *PostgresJobRepository) ClaimForExecution(
	ctx context.Context,
	jobID string,
	expectedStatus job.Status,
	expectedAttempts int,
) (bool, error) {
	query := `
		UPDATE jobs
		SET
			status = 'running',
			attempts = attempts + 1,
			next_attempt_at = NULL,
			updated_at = NOW()
		WHERE id = $1
			AND status = $2
			AND attempts = $3
	`

	result, err := r.pool.Exec(
		ctx,
		query,
		jobID,
		string(expectedStatus),
		expectedAttempts,
	)
	if err != nil {
		return false, err
	}

	return result.RowsAffected() == 1, nil
}
