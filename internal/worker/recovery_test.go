package worker

import (
	"context"
	"testing"
	"time"

	"github.com/sindhoora-17/taskforge/internal/queue"
)

type recoveryTestQueue struct {
	messages     []queue.Message
	nextCursor   string
	consumerName string
	minIdle      time.Duration
	start        string
	count        int64
}

func (q *recoveryTestQueue) ClaimStale(
	_ context.Context,
	consumerName string,
	minIdle time.Duration,
	start string,
	count int64,
) ([]queue.Message, string, error) {
	q.consumerName = consumerName
	q.minIdle = minIdle
	q.start = start
	q.count = count

	return q.messages, q.nextCursor, nil
}

func (q *recoveryTestQueue) RefreshPending(
	_ context.Context,
	_ string,
	_ string,
) error {
	return nil
}

type recoveryTestProcessor struct {
	processed []queue.Message
}

func (p *recoveryTestProcessor) Process(
	_ context.Context,
	message queue.Message,
) error {
	p.processed = append(p.processed, message)
	return nil
}

func TestRecoveryProcessesClaimedMessage(t *testing.T) {
	message := queue.Message{
		ID:    "message-123",
		JobID: "job-123",
		Type:  "slow_task",
	}

	recoveryQueue := &recoveryTestQueue{
		messages:   []queue.Message{message},
		nextCursor: "42-0",
	}

	processor := &recoveryTestProcessor{}

	recovery := NewRecovery(
		recoveryQueue,
		processor,
		"recovery-consumer",
		15*time.Second,
		5*time.Second,
		100,
	)

	cursor := "0-0"
	recovery.recover(context.Background(), &cursor)

	if recoveryQueue.consumerName != "recovery-consumer" {
		t.Fatalf(
			"expected recovery-consumer, got %s",
			recoveryQueue.consumerName,
		)
	}

	if recoveryQueue.minIdle != 15*time.Second {
		t.Fatalf(
			"expected 15 second minimum idle time, got %s",
			recoveryQueue.minIdle,
		)
	}

	if recoveryQueue.start != "0-0" {
		t.Fatalf(
			"expected cursor 0-0, got %s",
			recoveryQueue.start,
		)
	}

	if recoveryQueue.count != 100 {
		t.Fatalf(
			"expected batch size 100, got %d",
			recoveryQueue.count,
		)
	}

	if cursor != "42-0" {
		t.Fatalf(
			"expected cursor 42-0, got %s",
			cursor,
		)
	}

	if len(processor.processed) != 1 {
		t.Fatalf(
			"expected 1 processed message, got %d",
			len(processor.processed),
		)
	}

	if processor.processed[0].JobID != "job-123" {
		t.Fatalf(
			"expected job-123, got %s",
			processor.processed[0].JobID,
		)
	}
}
