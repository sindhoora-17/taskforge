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
	DefaultStream           = "taskforge:jobs"
	DefaultConsumerGroup    = "taskforge-workers"
	DefaultRetrySet         = "taskforge:retries"
	DefaultDeadLetterStream = "taskforge:dead-letter"
)

var ErrNoMessage = errors.New("no message available")

type Message struct {
	ID          string          `json:"message_id"`
	JobID       string          `json:"job_id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts int             `json:"max_attempts"`
	Recovered   bool            `json:"-"`
}

type retryEntry struct {
	JobID       string `json:"job_id"`
	Type        string `json:"type"`
	Payload     string `json:"payload"`
	MaxAttempts int    `json:"max_attempts"`
}

type RedisQueue struct {
	client           *redis.Client
	stream           string
	group            string
	retrySet         string
	deadLetterStream string
}

func NewRedisQueue(address string) *RedisQueue {
	client := redis.NewClient(&redis.Options{
		Addr: address,
	})

	return &RedisQueue{
		client:           client,
		stream:           DefaultStream,
		group:            DefaultConsumerGroup,
		retrySet:         DefaultRetrySet,
		deadLetterStream: DefaultDeadLetterStream,
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

	return parseStreamMessage(streams[0].Messages[0])
}

func (q *RedisQueue) ClaimStale(
	ctx context.Context,
	consumerName string,
	minIdle time.Duration,
	start string,
	count int64,
) ([]Message, string, error) {
	streamMessages, nextStart, err := q.client.XAutoClaim(
		ctx,
		&redis.XAutoClaimArgs{
			Stream:   q.stream,
			Group:    q.group,
			Consumer: consumerName,
			MinIdle:  minIdle,
			Start:    start,
			Count:    count,
		},
	).Result()

	if errors.Is(err, redis.Nil) {
		return nil, "0-0", nil
	}

	if err != nil {
		return nil, start, err
	}

	messages := make([]Message, 0, len(streamMessages))

	for _, streamMessage := range streamMessages {
		message, err := parseStreamMessage(streamMessage)
		if err != nil {
			return nil, nextStart, fmt.Errorf(
				"parse claimed message %s: %w",
				streamMessage.ID,
				err,
			)
		}

		message.Recovered = true
		messages = append(messages, message)
	}

	return messages, nextStart, nil
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

func (q *RedisQueue) RefreshPending(
	ctx context.Context,
	consumerName string,
	messageID string,
) error {
	claimedIDs, err := q.client.XClaimJustID(
		ctx,
		&redis.XClaimArgs{
			Stream:   q.stream,
			Group:    q.group,
			Consumer: consumerName,
			MinIdle:  0,
			Messages: []string{messageID},
		},
	).Result()
	if err != nil {
		return fmt.Errorf("refresh pending message: %w", err)
	}

	if len(claimedIDs) == 0 {
		return errors.New("message is no longer pending")
	}

	return nil
}

func (q *RedisQueue) ScheduleRetry(
	ctx context.Context,
	message Message,
	nextAttemptAt time.Time,
) error {
	entry := retryEntry{
		JobID:       message.JobID,
		Type:        message.Type,
		Payload:     string(message.Payload),
		MaxAttempts: message.MaxAttempts,
	}

	encodedEntry, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("encode retry entry: %w", err)
	}

	_, err = q.client.TxPipelined(
		ctx,
		func(pipe redis.Pipeliner) error {
			pipe.ZAdd(ctx, q.retrySet, redis.Z{
				Score:  float64(nextAttemptAt.UnixMilli()),
				Member: string(encodedEntry),
			})

			pipe.XAck(
				ctx,
				q.stream,
				q.group,
				message.ID,
			)

			return nil
		},
	)

	if err != nil {
		return fmt.Errorf("schedule retry: %w", err)
	}

	return nil
}

func (q *RedisQueue) MoveToDeadLetter(
	ctx context.Context,
	message Message,
	attempts int,
	lastError string,
) error {
	_, err := q.client.TxPipelined(
		ctx,
		func(pipe redis.Pipeliner) error {
			pipe.XAdd(ctx, &redis.XAddArgs{
				Stream: q.deadLetterStream,
				Values: map[string]any{
					"job_id":              message.JobID,
					"type":                message.Type,
					"payload":             string(message.Payload),
					"attempts":            attempts,
					"max_attempts":        message.MaxAttempts,
					"last_error":          lastError,
					"failed_at":           time.Now().UTC().Format(time.RFC3339Nano),
					"original_message_id": message.ID,
				},
			})

			pipe.XAck(
				ctx,
				q.stream,
				q.group,
				message.ID,
			)

			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("move job to dead-letter queue: %w", err)
	}

	return nil
}

var promoteRetriesScript = redis.NewScript(`
	local entries = redis.call(
		'ZRANGEBYSCORE',
		KEYS[1],
		'-inf',
		ARGV[1],
		'LIMIT',
		0,
		ARGV[2]
	)

	for _, entry in ipairs(entries) do
		local job = cjson.decode(entry)

		redis.call(
			'XADD',
			KEYS[2],
			'*',
			'job_id', job.job_id,
			'type', job.type,
			'payload', job.payload,
			'max_attempts', tostring(job.max_attempts)
		)

		redis.call('ZREM', KEYS[1], entry)
	end

	return #entries
`)

func (q *RedisQueue) PromoteDueRetries(
	ctx context.Context,
	now time.Time,
	limit int,
) (int64, error) {
	count, err := promoteRetriesScript.Run(
		ctx,
		q.client,
		[]string{q.retrySet, q.stream},
		now.UnixMilli(),
		limit,
	).Int64()

	if err != nil {
		return 0, fmt.Errorf("promote due retries: %w", err)
	}

	return count, nil
}

func (q *RedisQueue) Close() error {
	return q.client.Close()
}

func parseStreamMessage(
	streamMessage redis.XMessage,
) (Message, error) {
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
		return Message{}, errors.New(
			"message contains invalid JSON payload",
		)
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
