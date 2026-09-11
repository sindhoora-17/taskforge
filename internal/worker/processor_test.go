package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/sindhoora-17/taskforge/internal/job"
	"github.com/sindhoora-17/taskforge/internal/queue"
)

type statusUpdate struct {
	status   job.Status
	attempts int
}

type fakeRepository struct {
	storedJob     job.Job
	getError      error
	updates       []statusUpdate
	lastError     string
	nextAttemptAt time.Time
	claimRejected bool
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

func (f *fakeRepository) ClaimForExecution(
	_ context.Context,
	_ string,
	expectedStatus job.Status,
	expectedAttempts int,
) (bool, error) {
	if f.claimRejected {
		return false, nil
	}

	if f.storedJob.Status != expectedStatus ||
		f.storedJob.Attempts != expectedAttempts {
		return false, nil
	}

	attempts := expectedAttempts + 1

	f.updates = append(f.updates, statusUpdate{
		status:   job.StatusRunning,
		attempts: attempts,
	})

	f.storedJob.Status = job.StatusRunning
	f.storedJob.Attempts = attempts

	return true, nil
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

func (f *fakeRepository) MarkRetrying(
	_ context.Context,
	_ string,
	attempts int,
	lastError string,
	nextAttemptAt time.Time,
) error {
	f.updates = append(f.updates, statusUpdate{
		status:   job.StatusRetrying,
		attempts: attempts,
	})

	f.lastError = lastError
	f.nextAttemptAt = nextAttemptAt

	return nil
}

func (f *fakeRepository) MarkFailed(
	_ context.Context,
	_ string,
	attempts int,
	_ string,
) error {
	f.updates = append(f.updates, statusUpdate{
		status:   job.StatusFailed,
		attempts: attempts,
	})

	return nil
}

type deadLetterRecord struct {
	message   queue.Message
	attempts  int
	lastError string
}

type fakeQueue struct {
	acknowledged []string
	deadLettered []deadLetterRecord
}

func (f *fakeQueue) Acknowledge(
	_ context.Context,
	messageID string,
) error {
	f.acknowledged = append(f.acknowledged, messageID)
	return nil
}

func (f *fakeQueue) ScheduleRetry(
	_ context.Context,
	message queue.Message,
	_ time.Time,
) error {
	f.acknowledged = append(f.acknowledged, message.ID)
	return nil
}

func (f *fakeQueue) MoveToDeadLetter(
	_ context.Context,
	message queue.Message,
	attempts int,
	lastError string,
) error {
	f.deadLettered = append(
		f.deadLettered,
		deadLetterRecord{
			message:   message,
			attempts:  attempts,
			lastError: lastError,
		},
	)

	return nil
}

type fakeExecutor struct {
	called bool
	err    error
}

type contextBlockingExecutor struct{}

func (e *contextBlockingExecutor) Execute(
	ctx context.Context,
	_ string,
	_ string,
	_ json.RawMessage,
	_ int,
) error {
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeExecutor) Execute(
	_ context.Context,
	_ string,
	_ string,
	_ json.RawMessage,
	_ int,
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
			ID:          "job-123",
			Type:        "generate_report",
			Payload:     json.RawMessage(`{"report_name":""}`),
			Status:      job.StatusQueued,
			Attempts:    1,
			MaxAttempts: 2,
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
		ID:          "message-1",
		JobID:       "job-123",
		Type:        "generate_report",
		Payload:     json.RawMessage(`{"report_name":""}`),
		MaxAttempts: 2,
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

	if len(messageQueue.deadLettered) != 1 {
		t.Fatalf(
			"expected one dead-lettered message, got %d",
			len(messageQueue.deadLettered),
		)
	}

	deadLetter := messageQueue.deadLettered[0]

	if deadLetter.message.ID != "message-1" {
		t.Errorf(
			"expected message-1, got %s",
			deadLetter.message.ID,
		)
	}

	if deadLetter.attempts != 2 {
		t.Errorf(
			"expected 2 attempts, got %d",
			deadLetter.attempts,
		)
	}

	if deadLetter.lastError != "execution failed" {
		t.Errorf(
			"expected execution failed error, got %s",
			deadLetter.lastError,
		)
	}

	if len(messageQueue.acknowledged) != 0 {
		t.Error("dead-lettered message should not be acknowledged separately")
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

func TestProcessorSchedulesRetry(t *testing.T) {
	fixedTime := time.Date(
		2026,
		time.September,
		4,
		12,
		0,
		0,
		0,
		time.UTC,
	)

	repository := &fakeRepository{
		storedJob: job.Job{
			ID:          "job-123",
			Type:        "flaky_task",
			Payload:     json.RawMessage(`{"failures_before_success":2}`),
			Status:      job.StatusQueued,
			Attempts:    0,
			MaxAttempts: 3,
		},
	}

	messageQueue := &fakeQueue{}
	jobExecutor := &fakeExecutor{
		err: errors.New("temporary failure"),
	}

	processor := NewProcessor(
		repository,
		messageQueue,
		jobExecutor,
	)

	processor.now = func() time.Time {
		return fixedTime
	}

	err := processor.Process(context.Background(), queue.Message{
		ID:          "message-1",
		JobID:       "job-123",
		Type:        "flaky_task",
		Payload:     json.RawMessage(`{"failures_before_success":2}`),
		MaxAttempts: 3,
	})

	if err == nil {
		t.Fatal("expected the execution attempt to fail")
	}

	if len(repository.updates) != 2 {
		t.Fatalf(
			"expected running and retrying updates, got %d",
			len(repository.updates),
		)
	}

	if repository.updates[0].status != job.StatusRunning {
		t.Errorf(
			"expected first status running, got %s",
			repository.updates[0].status,
		)
	}

	if repository.updates[1].status != job.StatusRetrying {
		t.Errorf(
			"expected second status retrying, got %s",
			repository.updates[1].status,
		)
	}

	expectedRetryTime := fixedTime.Add(time.Second)

	if !repository.nextAttemptAt.Equal(expectedRetryTime) {
		t.Errorf(
			"expected retry at %s, got %s",
			expectedRetryTime,
			repository.nextAttemptAt,
		)
	}

	if repository.lastError != "temporary failure" {
		t.Errorf(
			"expected stored error temporary failure, got %s",
			repository.lastError,
		)
	}

	if len(messageQueue.acknowledged) != 1 {
		t.Error("expected original message to be acknowledged")
	}
}

func TestCalculateRetryDelay(t *testing.T) {
	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{attempt: 1, expected: time.Second},
		{attempt: 2, expected: 2 * time.Second},
		{attempt: 3, expected: 4 * time.Second},
		{attempt: 4, expected: 8 * time.Second},
		{attempt: 7, expected: time.Minute},
	}

	for _, test := range tests {
		actual := calculateRetryDelay(test.attempt)

		if actual != test.expected {
			t.Errorf(
				"attempt %d: expected %s, got %s",
				test.attempt,
				test.expected,
				actual,
			)
		}
	}
}

func TestProcessorSkipsJobWhenAtomicClaimFails(t *testing.T) {
	repository := &fakeRepository{
		storedJob: job.Job{
			ID:          "job-123",
			Type:        "generate_report",
			Payload:     json.RawMessage(`{"report_name":"test"}`),
			Status:      job.StatusQueued,
			Attempts:    0,
			MaxAttempts: 3,
		},
		claimRejected: true,
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
		t.Fatalf("expected duplicate claim to be handled, got %v", err)
	}

	if jobExecutor.called {
		t.Error("job should not execute when atomic claim fails")
	}

	if len(repository.updates) != 0 {
		t.Error("unclaimed job should not receive status updates")
	}

	if len(messageQueue.acknowledged) != 1 {
		t.Error("duplicate message should be acknowledged")
	}
}

func TestProcessorTimesOutExecution(t *testing.T) {
	repository := &fakeRepository{
		storedJob: job.Job{
			ID:             "job-timeout",
			Type:           "slow_task",
			Payload:        json.RawMessage(`{"duration_ms":10000}`),
			Status:         job.StatusQueued,
			Attempts:       0,
			MaxAttempts:    1,
			TimeoutSeconds: 30,
		},
	}

	messageQueue := &fakeQueue{}
	jobExecutor := &contextBlockingExecutor{}

	processor := NewProcessor(
		repository,
		messageQueue,
		jobExecutor,
	)

	processor.timeoutFor = func(job.Job) time.Duration {
		return 10 * time.Millisecond
	}

	err := processor.Process(
		context.Background(),
		queue.Message{
			ID:             "message-timeout",
			JobID:          "job-timeout",
			Type:           "slow_task",
			Payload:        json.RawMessage(`{"duration_ms":10000}`),
			MaxAttempts:    1,
			TimeoutSeconds: 30,
		},
	)

	if err == nil {
		t.Fatal("expected timed-out execution to return an error")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf(
			"expected context deadline exceeded, got %v",
			err,
		)
	}

	if len(repository.updates) != 2 {
		t.Fatalf(
			"expected running and failed updates, got %d",
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

	if len(messageQueue.deadLettered) != 1 {
		t.Fatalf(
			"expected one dead-letter record, got %d",
			len(messageQueue.deadLettered),
		)
	}

	if messageQueue.deadLettered[0].attempts != 1 {
		t.Errorf(
			"expected one recorded attempt, got %d",
			messageQueue.deadLettered[0].attempts,
		)
	}
}
