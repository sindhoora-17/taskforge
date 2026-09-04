package worker

import (
	"context"
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
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	log.Println("Retry scheduler started")
	defer log.Println("Retry scheduler stopped")

	s.promote(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.promote(ctx)
		}
	}
}

func (s *RetryScheduler) promote(ctx context.Context) {
	count, err := s.queue.PromoteDueRetries(
		ctx,
		s.now().UTC(),
		s.batchSize,
	)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("failed to promote due retries: %v", err)
		}

		return
	}

	if count > 0 {
		log.Printf("Promoted %d due retry jobs", count)
	}
}
