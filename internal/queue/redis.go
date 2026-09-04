package queue

import (
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/sindhoora-17/taskforge/internal/job"
)

const DefaultStream = "taskforge:jobs"

type RedisQueue struct {
	client *redis.Client
	stream string
}

func NewRedisQueue(address string) *RedisQueue {
	client := redis.NewClient(&redis.Options{
		Addr: address,
	})

	return &RedisQueue{
		client: client,
		stream: DefaultStream,
	}
}

func (q *RedisQueue) Ping(ctx context.Context) error {
	return q.client.Ping(ctx).Err()
}

func (q *RedisQueue) Enqueue(
	ctx context.Context,
	newJob job.Job,
) (string, error) {
	messageID, err := q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream,
		Values: map[string]any{
			"job_id":       newJob.ID,
			"type":         newJob.Type,
			"payload":      string(newJob.Payload),
			"max_attempts": newJob.MaxAttempts,
		},
	}).Result()

	if err != nil {
		return "", err
	}

	return messageID, nil
}

func (q *RedisQueue) Close() error {
	return q.client.Close()
}
