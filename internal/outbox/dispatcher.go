package outbox

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sindhoora-17/taskforge/internal/job"
)

const (
	defaultBatchSize      = 100
	basePublishRetryDelay = time.Second
	maxPublishRetryDelay  = time.Minute
)

type Publisher interface {
	Enqueue(
		ctx context.Context,
		newJob job.Job,
	) (string, error)
}

type Dispatcher struct {
	pool      *pgxpool.Pool
	publisher Publisher
	interval  time.Duration
	batchSize int
	now       func() time.Time
}

func NewDispatcher(
	pool *pgxpool.Pool,
	publisher Publisher,
	interval time.Duration,
) *Dispatcher {
	return &Dispatcher{
		pool:      pool,
		publisher: publisher,
		interval:  interval,
		batchSize: defaultBatchSize,
		now:       time.Now,
	}
}

func (d *Dispatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	log.Println("Outbox dispatcher started")
	defer log.Println("Outbox dispatcher stopped")

	d.dispatchAvailable(ctx)

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			d.dispatchAvailable(ctx)
		}
	}
}

func (d *Dispatcher) dispatchAvailable(ctx context.Context) {
	for count := 0; count < d.batchSize; count++ {
		published, err := d.dispatchNext(ctx)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("Outbox dispatch failed: %v", err)
			}

			return
		}

		if !published {
			return
		}
	}
}

func (d *Dispatcher) dispatchNext(
	ctx context.Context,
) (bool, error) {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf(
			"begin outbox transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	query := `
		SELECT
			outbox.id,
			outbox.publish_attempts,
			jobs.id::text,
			jobs.type,
			jobs.payload,
			jobs.status,
			jobs.attempts,
			jobs.max_attempts,
			jobs.created_at,
			jobs.updated_at
		FROM job_outbox AS outbox
		JOIN jobs
			ON jobs.id = outbox.job_id
		WHERE outbox.published_at IS NULL
  AND (
      outbox.next_attempt_at IS NULL
      OR outbox.next_attempt_at <= NOW()
  )
ORDER BY
    outbox.next_attempt_at NULLS FIRST,
    outbox.created_at,
    outbox.id
		LIMIT 1
		FOR UPDATE OF outbox SKIP LOCKED
	`

	var outboxID int64
	var storedJob job.Job
	var status string
	var publishAttempts int

	err = tx.QueryRow(ctx, query).Scan(
		&outboxID,
		&publishAttempts,
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
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf(
			"select pending outbox event: %w",
			err,
		)
	}

	storedJob.Status = job.Status(status)

	if _, err := d.publisher.Enqueue(ctx, storedJob); err != nil {
		nextPublishAttempt := publishAttempts + 1

		nextAttemptAt := d.now().
			UTC().
			Add(calculatePublishRetryDelay(nextPublishAttempt))

		if _, updateErr := tx.Exec(
			ctx,
			`
			UPDATE job_outbox
			SET
				publish_attempts = $2,
				last_error = $3,
				next_attempt_at = $4
			WHERE id = $1
		`,
			outboxID,
			nextPublishAttempt,
			err.Error(),
			nextAttemptAt,
		); updateErr != nil {
			return false, fmt.Errorf(
				"publish failed (%v) and outbox failure update failed: %w",
				err,
				updateErr,
			)
		}

		if commitErr := tx.Commit(ctx); commitErr != nil {
			return false, fmt.Errorf(
				"publish failed (%v) and failure update commit failed: %w",
				err,
				commitErr,
			)
		}

		return false, fmt.Errorf(
			"publish outbox event %d: %w; retry scheduled for %s",
			outboxID,
			err,
			nextAttemptAt.Format(time.RFC3339Nano),
		)
	}

	if _, err := tx.Exec(
		ctx,
		`
			UPDATE job_outbox
			SET
				published_at = NOW(),
				publish_attempts = publish_attempts + 1,
				last_error = NULL,
				next_attempt_at = NULL
			WHERE id = $1
		`,
		outboxID,
	); err != nil {
		return false, fmt.Errorf(
			"mark outbox event as published: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf(
			"commit published outbox event: %w",
			err,
		)
	}

	log.Printf(
		"Published outbox event %d for job %s",
		outboxID,
		storedJob.ID,
	)

	return true, nil
}

func calculatePublishRetryDelay(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}

	exponent := attempts - 1

	if exponent >= 6 {
		return maxPublishRetryDelay
	}

	delay := basePublishRetryDelay * time.Duration(1<<exponent)

	if delay > maxPublishRetryDelay {
		return maxPublishRetryDelay
	}

	return delay
}
