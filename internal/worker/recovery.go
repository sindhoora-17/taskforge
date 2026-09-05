package worker

import (
	"context"
	"log"
	"time"

	"github.com/sindhoora-17/taskforge/internal/queue"
)

type StaleMessageClaimer interface {
	ClaimStale(
		ctx context.Context,
		consumerName string,
		minIdle time.Duration,
		start string,
		count int64,
	) ([]queue.Message, string, error)

	RefreshPending(
		ctx context.Context,
		consumerName string,
		messageID string,
	) error
}

type Recovery struct {
	queue        StaleMessageClaimer
	processor    MessageProcessor
	consumerName string
	minIdle      time.Duration
	interval     time.Duration
	batchSize    int64
}

func NewRecovery(
	queue StaleMessageClaimer,
	processor MessageProcessor,
	consumerName string,
	minIdle time.Duration,
	interval time.Duration,
	batchSize int64,
) *Recovery {
	return &Recovery{
		queue:        queue,
		processor:    processor,
		consumerName: consumerName,
		minIdle:      minIdle,
		interval:     interval,
		batchSize:    batchSize,
	}
}

func (r *Recovery) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	log.Printf(
		"Pending-message recovery %s started",
		r.consumerName,
	)
	defer log.Printf(
		"Pending-message recovery %s stopped",
		r.consumerName,
	)

	cursor := "0-0"

	r.recover(ctx, &cursor)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.recover(ctx, &cursor)
		}
	}
}

func (r *Recovery) recover(
	ctx context.Context,
	cursor *string,
) {
	messages, nextCursor, err := r.queue.ClaimStale(
		ctx,
		r.consumerName,
		r.minIdle,
		*cursor,
		r.batchSize,
	)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf(
				"Pending-message recovery failed: %v",
				err,
			)
		}

		return
	}

	*cursor = nextCursor

	for _, message := range messages {
		log.Printf(
			"Recovered stale job %s from message %s",
			message.JobID,
			message.ID,
		)

		if err := processWithHeartbeat(
			ctx,
			r.queue,
			r.processor,
			r.consumerName,
			message,
			5*time.Second,
		); err != nil {
			log.Printf(
				"Recovered job %s failed: %v",
				message.JobID,
				err,
			)
			continue
		}

		log.Printf(
			"Recovered job %s completed",
			message.JobID,
		)
	}
}
