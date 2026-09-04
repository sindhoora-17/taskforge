package worker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/sindhoora-17/taskforge/internal/job"
	"github.com/sindhoora-17/taskforge/internal/queue"
)

type Repository interface {
	GetByID(ctx context.Context, jobID string) (job.Job, error)
	UpdateStatus(
		ctx context.Context,
		jobID string,
		status job.Status,
		attempts int,
	) error
}

type MessageQueue interface {
	Acknowledge(ctx context.Context, messageID string) error
}

type Executor interface {
	Execute(
		ctx context.Context,
		jobID string,
		jobType string,
		payload json.RawMessage,
	) error
}

type Processor struct {
	repository Repository
	queue      MessageQueue
	executor   Executor
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

	// A message may be delivered more than once. If the job already
	// completed, acknowledge it without executing it again.
	if storedJob.Status == job.StatusCompleted {
		if err := p.queue.Acknowledge(ctx, message.ID); err != nil {
			return fmt.Errorf("acknowledge completed job: %w", err)
		}

		return nil
	}

	attempts := storedJob.Attempts + 1

	if err := p.repository.UpdateStatus(
		ctx,
		storedJob.ID,
		job.StatusRunning,
		attempts,
	); err != nil {
		return fmt.Errorf("mark job as running: %w", err)
	}

	executionErr := p.executor.Execute(
		ctx,
		storedJob.ID,
		storedJob.Type,
		storedJob.Payload,
	)

	if executionErr != nil {
		if err := p.repository.UpdateStatus(
			ctx,
			storedJob.ID,
			job.StatusFailed,
			attempts,
		); err != nil {
			return fmt.Errorf(
				"execution failed (%v) and status update failed: %w",
				executionErr,
				err,
			)
		}

		if err := p.queue.Acknowledge(ctx, message.ID); err != nil {
			return fmt.Errorf(
				"execution failed (%v) and acknowledgement failed: %w",
				executionErr,
				err,
			)
		}

		return fmt.Errorf("execute job: %w", executionErr)
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
