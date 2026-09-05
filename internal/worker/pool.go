package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/sindhoora-17/taskforge/internal/queue"
)

type QueueReader interface {
	Read(
		ctx context.Context,
		consumerName string,
	) (queue.Message, error)

	RefreshPending(
		ctx context.Context,
		consumerName string,
		messageID string,
	) error
}

type MessageProcessor interface {
	Process(ctx context.Context, message queue.Message) error
}

type Pool struct {
	queue          QueueReader
	processor      MessageProcessor
	consumerPrefix string
	concurrency    int
}

func NewPool(
	queue QueueReader,
	processor MessageProcessor,
	consumerPrefix string,
	concurrency int,
) *Pool {
	return &Pool{
		queue:          queue,
		processor:      processor,
		consumerPrefix: consumerPrefix,
		concurrency:    concurrency,
	}
}

func (p *Pool) Run(ctx context.Context) {
	var waitGroup sync.WaitGroup

	for workerNumber := 1; workerNumber <= p.concurrency; workerNumber++ {
		waitGroup.Add(1)

		consumerName := fmt.Sprintf(
			"%s-%d",
			p.consumerPrefix,
			workerNumber,
		)

		go func() {
			defer waitGroup.Done()
			p.runConsumer(ctx, consumerName)
		}()
	}

	waitGroup.Wait()
}

func (p *Pool) runConsumer(
	ctx context.Context,
	consumerName string,
) {
	log.Printf("Consumer %s started", consumerName)

	defer log.Printf("Consumer %s stopped", consumerName)

	for {
		if ctx.Err() != nil {
			return
		}

		message, err := p.queue.Read(ctx, consumerName)

		if errors.Is(err, queue.ErrNoMessage) {
			continue
		}

		if err != nil {
			if ctx.Err() != nil {
				return
			}

			log.Printf(
				"Consumer %s failed to read message: %v",
				consumerName,
				err,
			)

			timer := time.NewTimer(time.Second)

			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}

			continue
		}

		log.Printf(
			"Consumer %s received job %s (%s)",
			consumerName,
			message.JobID,
			message.Type,
		)

		if err := processWithHeartbeat(
			ctx,
			p.queue,
			p.processor,
			consumerName,
			message,
			5*time.Second,
		); err != nil {
			log.Printf(
				"Consumer %s failed job %s: %v",
				consumerName,
				message.JobID,
				err,
			)
			continue
		}

		log.Printf(
			"Consumer %s completed job %s",
			consumerName,
			message.JobID,
		)
	}
}
