package worker

import (
	"context"
	"testing"
	"time"

	"github.com/sindhoora-17/taskforge/internal/queue"
)

type heartbeatTestCall struct {
	consumerName string
	messageID    string
}

type heartbeatTestRefresher struct {
	calls chan heartbeatTestCall
}

func (r *heartbeatTestRefresher) RefreshPending(
	_ context.Context,
	consumerName string,
	messageID string,
) error {
	r.calls <- heartbeatTestCall{
		consumerName: consumerName,
		messageID:    messageID,
	}

	return nil
}

type heartbeatTestProcessor struct {
	started chan struct{}
	release chan struct{}
}

func (p *heartbeatTestProcessor) Process(
	ctx context.Context,
	_ queue.Message,
) error {
	close(p.started)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.release:
		return nil
	}
}

func TestProcessWithHeartbeatRefreshesPendingMessage(
	t *testing.T,
) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	refresher := &heartbeatTestRefresher{
		calls: make(chan heartbeatTestCall, 10),
	}

	processor := &heartbeatTestProcessor{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}

	message := queue.Message{
		ID:    "message-123",
		JobID: "job-123",
		Type:  "slow_task",
	}

	result := make(chan error, 1)

	go func() {
		result <- processWithHeartbeat(
			ctx,
			refresher,
			processor,
			"worker-1",
			message,
			10*time.Millisecond,
		)
	}()

	select {
	case <-processor.started:
	case <-ctx.Done():
		t.Fatal("processor did not start")
	}

	select {
	case call := <-refresher.calls:
		if call.consumerName != "worker-1" {
			t.Fatalf(
				"expected consumer worker-1, got %s",
				call.consumerName,
			)
		}

		if call.messageID != "message-123" {
			t.Fatalf(
				"expected message message-123, got %s",
				call.messageID,
			)
		}

	case <-ctx.Done():
		t.Fatal("heartbeat was not sent")
	}

	close(processor.release)

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("process returned an error: %v", err)
		}

	case <-ctx.Done():
		t.Fatal("processing did not finish")
	}
}
