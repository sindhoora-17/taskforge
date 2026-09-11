package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/sindhoora-17/taskforge/internal/job"
	"github.com/sindhoora-17/taskforge/internal/queue"
)

const (
	baseRetryDelay    = time.Second
	maxRetryDelay     = time.Minute
	defaultJobTimeout = 30 * time.Second
)

type Repository interface {
	GetByID(
		ctx context.Context,
		jobID string,
	) (job.Job, error)

	ClaimForExecution(
		ctx context.Context,
		jobID string,
		expectedStatus job.Status,
		expectedAttempts int,
	) (bool, error)

	UpdateStatus(
		ctx context.Context,
		jobID string,
		status job.Status,
		attempts int,
	) error

	MarkRetrying(
		ctx context.Context,
		jobID string,
		attempts int,
		lastError string,
		nextAttemptAt time.Time,
	) error

	MarkFailed(
		ctx context.Context,
		jobID string,
		attempts int,
		lastError string,
	) error
}

type MessageQueue interface {
	Acknowledge(
		ctx context.Context,
		messageID string,
	) error

	ScheduleRetry(
		ctx context.Context,
		message queue.Message,
		nextAttemptAt time.Time,
	) error

	MoveToDeadLetter(
		ctx context.Context,
		message queue.Message,
		attempts int,
		lastError string,
	) error
}

type Executor interface {
	Execute(
		ctx context.Context,
		jobID string,
		jobType string,
		payload json.RawMessage,
		attempt int,
	) error
}

type Processor struct {
	repository Repository
	queue      MessageQueue
	executor   Executor
	now        func() time.Time
	timeoutFor func(job.Job) time.Duration
}

func NewProcessor(
	repository Repository,
	queue MessageQueue,
	executor Executor,
) *Processor {
	return &Processor{
		repository: repository,
		queue:      queue,
		executor:   executor,
		now:        time.Now,
		timeoutFor: func(storedJob job.Job) time.Duration {
			if storedJob.TimeoutSeconds <= 0 {
				return defaultJobTimeout
			}

			return time.Duration(
				storedJob.TimeoutSeconds,
			) * time.Second
		},
	}
}

func (p *Processor) Process(
	ctx context.Context,
	message queue.Message,
) error {
	storedJob, err := p.repository.GetByID(ctx, message.JobID)
	if err != nil {
		return fmt.Errorf("retrieve job: %w", err)
	}

	if storedJob.Status == job.StatusCompleted ||
		storedJob.Status == job.StatusFailed {
		if err := p.queue.Acknowledge(ctx, message.ID); err != nil {
			return fmt.Errorf("acknowledge terminal job: %w", err)
		}

		return nil
	}

	if storedJob.Status == job.StatusRunning && !message.Recovered {
		if err := p.queue.Acknowledge(ctx, message.ID); err != nil {
			return fmt.Errorf("acknowledge duplicate message: %w", err)
		}

		return nil
	}

	claimed, err := p.repository.ClaimForExecution(
		ctx,
		storedJob.ID,
		storedJob.Status,
		storedJob.Attempts,
	)
	if err != nil {
		return fmt.Errorf("claim job for execution: %w", err)
	}

	if !claimed {
		if err := p.queue.Acknowledge(ctx, message.ID); err != nil {
			return fmt.Errorf("acknowledge unclaimed message: %w", err)
		}

		return nil
	}

	attempts := storedJob.Attempts + 1
	executionTimeout := p.timeoutFor(storedJob)

	executionContext, cancelExecution := context.WithTimeout(
		ctx,
		executionTimeout,
	)

	executionErr := p.executor.Execute(
		executionContext,
		storedJob.ID,
		storedJob.Type,
		storedJob.Payload,
		attempts,
	)

	executionContextErr := executionContext.Err()
	cancelExecution()

	if errors.Is(executionContextErr, context.DeadlineExceeded) {
		executionErr = fmt.Errorf(
			"job execution timed out after %s: %w",
			executionTimeout,
			context.DeadlineExceeded,
		)
	}

	if executionErr != nil {
		if attempts < storedJob.MaxAttempts {
			nextAttemptAt := p.now().
				UTC().
				Add(calculateRetryDelay(attempts))

			if err := p.repository.MarkRetrying(
				ctx,
				storedJob.ID,
				attempts,
				executionErr.Error(),
				nextAttemptAt,
			); err != nil {
				return fmt.Errorf(
					"execution failed (%v) and retry state update failed: %w",
					executionErr,
					err,
				)
			}

			if err := p.queue.ScheduleRetry(
				ctx,
				message,
				nextAttemptAt,
			); err != nil {
				return fmt.Errorf(
					"execution failed (%v) and retry scheduling failed: %w",
					executionErr,
					err,
				)
			}

			return fmt.Errorf(
				"execute job: %w; retry scheduled for %s",
				executionErr,
				nextAttemptAt.Format(time.RFC3339Nano),
			)
		}

		if err := p.repository.MarkFailed(
			ctx,
			storedJob.ID,
			attempts,
			executionErr.Error(),
		); err != nil {
			return fmt.Errorf(
				"execution failed (%v) and final status update failed: %w",
				executionErr,
				err,
			)
		}

		if err := p.queue.MoveToDeadLetter(
			ctx,
			message,
			attempts,
			executionErr.Error(),
		); err != nil {
			return fmt.Errorf(
				"execution failed (%v) and dead-letter operation failed: %w",
				executionErr,
				err,
			)
		}

		return fmt.Errorf(
			"execute job after %d attempts: %w",
			attempts,
			executionErr,
		)
	}

	if err := p.repository.UpdateStatus(
		ctx,
		storedJob.ID,
		job.StatusCompleted,
		attempts,
	); err != nil {
		return fmt.Errorf("mark job as completed: %w", err)
	}

	if err := p.queue.Acknowledge(ctx, message.ID); err != nil {
		return fmt.Errorf("acknowledge message: %w", err)
	}

	return nil
}

func calculateRetryDelay(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}

	delay := baseRetryDelay * time.Duration(1<<(attempts-1))

	if delay > maxRetryDelay {
		return maxRetryDelay
	}

	return delay
}
