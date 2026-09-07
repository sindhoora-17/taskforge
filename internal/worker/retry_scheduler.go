package worker

import (
	"context"
	"fmt"
	"log"
	"time"
)

type RetryPromoter interface {
	PromoteDueRetries(
		ctx context.Context,
		now time.Time,
		limit int,
	) (int64, error)
}

type RetryScheduler struct {
	queue     RetryPromoter
	interval  time.Duration
	batchSize int
	now       func() time.Time
}

func NewRetryScheduler(
	queue RetryPromoter,
	interval time.Duration,
	batchSize int,
) *RetryScheduler {
	return &RetryScheduler{
		queue:     queue,
		interval:  interval,
		batchSize: batchSize,
		now:       time.Now,
	}
}

func (s *RetryScheduler) Run(ctx context.Context) {
	log.Println("Retry scheduler started")
	defer log.Println("Retry scheduler stopped")

	timer := time.NewTimer(0)
	defer timer.Stop()

	consecutiveFailures := 0

	for {
		select {
		case <-ctx.Done():
			return

		case <-timer.C:
			err := s.promote(ctx)

			if err != nil {
				if ctx.Err() != nil {
					return
				}

				consecutiveFailures++

				retryDelay := calculateDependencyBackoff(
					consecutiveFailures,
				)

				log.Printf(
					"Failed to promote due retries: %v; retrying in %s",
					err,
					retryDelay,
				)

				timer.Reset(retryDelay)
				continue
			}

			consecutiveFailures = 0
			timer.Reset(s.interval)
		}
	}
}

func (s *RetryScheduler) promote(ctx context.Context) error {
	count, err := s.queue.PromoteDueRetries(
		ctx,
		s.now().UTC(),
		s.batchSize,
	)
	if err != nil {
		return fmt.Errorf("promote due retries: %w", err)
	}

	if count > 0 {
		log.Printf("Promoted %d due retry jobs", count)
	}

	return nil
}
