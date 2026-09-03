package repository

import (
	"context"
	"errors"

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
	query := `
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

	_, err := r.pool.Exec(
		ctx,
		query,
		newJob.ID,
		newJob.Type,
		newJob.Payload,
		string(newJob.Status),
		newJob.Attempts,
		newJob.MaxAttempts,
		newJob.CreatedAt,
		newJob.UpdatedAt,
	)

	return err
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
			updated_at
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
