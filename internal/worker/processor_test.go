package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/sindhoora-17/taskforge/internal/job"
	"github.com/sindhoora-17/taskforge/internal/queue"
)

type statusUpdate struct {
	status   job.Status
	attempts int
}

type fakeRepository struct {
	storedJob job.Job
	getError  error
	updates   []statusUpdate
}

func (f *fakeRepository) GetByID(
	_ context.Context,
	_ string,
) (job.Job, error) {
	if f.getError != nil {
		return job.Job{}, f.getError
	}

	return f.storedJob, nil
}

func (f *fakeRepository) UpdateStatus(
	_ context.Context,
	_ string,
	status job.Status,
	attempts int,
) error {
	f.updates = append(f.updates, statusUpdate{
		status:   status,
		attempts: attempts,
	})

	return nil
}

type fakeQueue struct {
	acknowledged []string
}

func (f *fakeQueue) Acknowledge(
	_ context.Context,
	messageID string,
) error {
	f.acknowledged = append(f.acknowledged, messageID)
	return nil
}

type fakeExecutor struct {
	called bool
	err    error
}

func (f *fakeExecutor) Execute(
	_ context.Context,
	_ string,
	_ string,
	_ json.RawMessage,
) error {
	f.called = true
	return f.err
}

func TestProcessorCompletesJob(t *testing.T) {
	repository := &fakeRepository{
		storedJob: job.Job{
			ID:       "job-123",
			Type:     "generate_report",
			Payload:  json.RawMessage(`{"report_name":"test"}`),
			Status:   job.StatusQueued,
			Attempts: 0,
		},
	}

	messageQueue := &fakeQueue{}
	jobExecutor := &fakeExecutor{}

	processor := NewProcessor(
		repository,
		messageQueue,
		jobExecutor,
	)

	message := queue.Message{
		ID:    "message-1",
		JobID: "job-123",
	}

	err := processor.Process(context.Background(), message)
	if err != nil {
		t.Fatalf("expected processing to succeed, got %v", err)
	}

	if !jobExecutor.called {
		t.Error("expected executor to be called")
	}

	if len(repository.updates) != 2 {
		t.Fatalf(
			"expected two status updates, got %d",
			len(repository.updates),
		)
	}

	if repository.updates[0].status != job.StatusRunning {
		t.Errorf(
			"expected first status to be running, got %s",
			repository.updates[0].status,
		)
	}

	if repository.updates[0].attempts != 1 {
		t.Errorf(
			"expected one attempt, got %d",
			repository.updates[0].attempts,
		)
	}

	if repository.updates[1].status != job.StatusCompleted {
		t.Errorf(
			"expected final status to be completed, got %s",
			repository.updates[1].status,
		)
	}

	if len(messageQueue.acknowledged) != 1 {
		t.Fatalf(
			"expected one acknowledgement, got %d",
			len(messageQueue.acknowledged),
		)
	}

	if messageQueue.acknowledged[0] != "message-1" {
		t.Errorf(
			"expected message-1 to be acknowledged, got %s",
			messageQueue.acknowledged[0],
		)
	}
}

func TestProcessorMarksFailedExecution(t *testing.T) {
	repository := &fakeRepository{
		storedJob: job.Job{
			ID:       "job-123",
			Type:     "generate_report",
			Payload:  json.RawMessage(`{"report_name":""}`),
			Status:   job.StatusQueued,
			Attempts: 0,
		},
	}

	messageQueue := &fakeQueue{}
	jobExecutor := &fakeExecutor{
		err: errors.New("execution failed"),
	}

	processor := NewProcessor(
		repository,
		messageQueue,
		jobExecutor,
	)

	err := processor.Process(context.Background(), queue.Message{
		ID:    "message-1",
		JobID: "job-123",
	})

	if err == nil {
		t.Fatal("expected processing to fail")
	}

	if len(repository.updates) != 2 {
		t.Fatalf(
			"expected two status updates, got %d",
			len(repository.updates),
		)
	}

	if repository.updates[0].status != job.StatusRunning {
		t.Errorf(
			"expected first status to be running, got %s",
			repository.updates[0].status,
		)
	}

	if repository.updates[1].status != job.StatusFailed {
		t.Errorf(
			"expected final status to be failed, got %s",
			repository.updates[1].status,
		)
	}

	if len(messageQueue.acknowledged) != 1 {
		t.Error("expected failed message to be acknowledged")
	}
}

func TestProcessorSkipsCompletedJob(t *testing.T) {
	repository := &fakeRepository{
		storedJob: job.Job{
			ID:       "job-123",
			Status:   job.StatusCompleted,
			Attempts: 1,
		},
	}

	messageQueue := &fakeQueue{}
	jobExecutor := &fakeExecutor{}

	processor := NewProcessor(
		repository,
		messageQueue,
		jobExecutor,
	)

	err := processor.Process(context.Background(), queue.Message{
		ID:    "message-1",
		JobID: "job-123",
	})
	if err != nil {
		t.Fatalf("expected duplicate message to be handled, got %v", err)
	}

	if jobExecutor.called {
		t.Error("completed job should not execute again")
	}

	if len(repository.updates) != 0 {
		t.Error("completed job should not receive status updates")
	}

	if len(messageQueue.acknowledged) != 1 {
		t.Error("expected duplicate message to be acknowledged")
	}
}
