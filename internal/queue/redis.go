package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sindhoora-17/taskforge/internal/job"
)

const (
	DefaultStream        = "taskforge:jobs"
	DefaultConsumerGroup = "taskforge-workers"
)

var ErrNoMessage = errors.New("no message available")

type Message struct {
	ID          string
	JobID       string
	Type        string
	Payload     json.RawMessage
	MaxAttempts int
}

type RedisQueue struct {
	client *redis.Client
	stream string
	group  string
}

func NewRedisQueue(address string) *RedisQueue {
	client := redis.NewClient(&redis.Options{
		Addr: address,
	})

	return &RedisQueue{
		client: client,
		stream: DefaultStream,
		group:  DefaultConsumerGroup,
	}
}

func (q *RedisQueue) Ping(ctx context.Context) error {
	return q.client.Ping(ctx).Err()
}

func (q *RedisQueue) CreateConsumerGroup(ctx context.Context) error {
	err := q.client.XGroupCreateMkStream(
		ctx,
		q.stream,
		q.group,
		"0",
	).Err()

	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}

	return nil
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

func (q *RedisQueue) Read(
	ctx context.Context,
	consumerName string,
) (Message, error) {
	streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.group,
		Consumer: consumerName,
		Streams:  []string{q.stream, ">"},
		Count:    1,
		Block:    5 * time.Second,
	}).Result()

	if errors.Is(err, redis.Nil) {
		return Message{}, ErrNoMessage
	}

	if err != nil {
		return Message{}, err
	}

	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return Message{}, ErrNoMessage
	}

	streamMessage := streams[0].Messages[0]

	jobID, err := readStringField(streamMessage.Values, "job_id")
	if err != nil {
		return Message{}, err
	}

	jobType, err := readStringField(streamMessage.Values, "type")
	if err != nil {
		return Message{}, err
	}

	payload, err := readStringField(streamMessage.Values, "payload")
	if err != nil {
		return Message{}, err
	}

	if !json.Valid([]byte(payload)) {
		return Message{}, errors.New("message contains invalid JSON payload")
	}

	maxAttemptsValue, err := readStringField(
		streamMessage.Values,
		"max_attempts",
	)
	if err != nil {
		return Message{}, err
	}

	maxAttempts, err := strconv.Atoi(maxAttemptsValue)
	if err != nil {
		return Message{}, fmt.Errorf(
			"invalid max_attempts value: %w",
			err,
		)
	}

	return Message{
		ID:          streamMessage.ID,
		JobID:       jobID,
		Type:        jobType,
		Payload:     json.RawMessage(payload),
		MaxAttempts: maxAttempts,
	}, nil
}

func (q *RedisQueue) Acknowledge(
	ctx context.Context,
	messageID string,
) error {
	return q.client.XAck(
		ctx,
		q.stream,
		q.group,
		messageID,
	).Err()
}

func (q *RedisQueue) Close() error {
	return q.client.Close()
}

func readStringField(
	values map[string]any,
	field string,
) (string, error) {
	value, exists := values[field]
	if !exists {
		return "", fmt.Errorf("message is missing field %q", field)
	}

	switch typedValue := value.(type) {
	case string:
		return typedValue, nil
	case []byte:
		return string(typedValue), nil
	default:
		return fmt.Sprint(typedValue), nil
	}
}
