package worker

import (
	"context"
	"log"
	"time"

	"github.com/sindhoora-17/taskforge/internal/queue"
)

type PendingMessageRefresher interface {
	RefreshPending(
		ctx context.Context,
		consumerName string,
		messageID string,
	) error
}

func processWithHeartbeat(
	ctx context.Context,
	refresher PendingMessageRefresher,
	processor MessageProcessor,
	consumerName string,
	message queue.Message,
	interval time.Duration,
) error {
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})

	go func() {
		defer close(heartbeatDone)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-heartbeatCtx.Done():
				return

			case <-ticker.C:
				err := refresher.RefreshPending(
					heartbeatCtx,
					consumerName,
					message.ID,
				)
				if err != nil && heartbeatCtx.Err() == nil {
					log.Printf(
						"Consumer %s failed to refresh job %s heartbeat: %v",
						consumerName,
						message.JobID,
						err,
					)
				}
			}
		}
	}()

	processErr := processor.Process(ctx, message)

	cancelHeartbeat()
	<-heartbeatDone

	return processErr
}
