package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/sindhoora-17/taskforge/internal/executor"
	"github.com/sindhoora-17/taskforge/internal/job"
	"github.com/sindhoora-17/taskforge/internal/queue"
	"github.com/sindhoora-17/taskforge/internal/repository"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println(".env file not found; using system environment variables")
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("failed to create database pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("failed to connect to PostgreSQL: %v", err)
	}

	redisAddress := os.Getenv("REDIS_ADDR")
	if redisAddress == "" {
		log.Fatal("REDIS_ADDR is required")
	}

	redisQueue := queue.NewRedisQueue(redisAddress)

	defer func() {
		if err := redisQueue.Close(); err != nil {
			log.Printf("failed to close Redis connection: %v", err)
		}
	}()

	if err := redisQueue.Ping(ctx); err != nil {
		log.Fatalf("failed to connect to Redis: %v", err)
	}

	if err := redisQueue.CreateConsumerGroup(ctx); err != nil {
		log.Fatalf("failed to create Redis consumer group: %v", err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("failed to determine hostname: %v", err)
	}

	consumerName := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	jobRepository := repository.NewPostgresJobRepository(pool)
	jobExecutor := executor.New("output")

	log.Printf("Worker %s started", consumerName)

	for {
		message, err := redisQueue.Read(ctx, consumerName)

		if errors.Is(err, queue.ErrNoMessage) {
			continue
		}

		if err != nil {
			if ctx.Err() != nil {
				break
			}

			log.Printf("failed to read queue message: %v", err)

			select {
			case <-ctx.Done():
				break
			case <-time.After(time.Second):
			}

			continue
		}

		log.Printf(
			"Worker %s received job %s (%s)",
			consumerName,
			message.JobID,
			message.Type,
		)

		if err := processMessage(
			ctx,
			jobRepository,
			redisQueue,
			jobExecutor,
			message,
		); err != nil {
			log.Printf("Job %s failed: %v", message.JobID, err)
			continue
		}

		log.Printf("Job %s completed", message.JobID)
	}

	log.Printf("Worker %s stopped", consumerName)
}

func processMessage(
	ctx context.Context,
	jobRepository *repository.PostgresJobRepository,
	redisQueue *queue.RedisQueue,
	jobExecutor *executor.Executor,
	message queue.Message,
) error {
	storedJob, err := jobRepository.GetByID(ctx, message.JobID)
	if err != nil {
		return fmt.Errorf("retrieve job: %w", err)
	}

	// If a completed job is delivered again, acknowledge it without
	// executing it a second time.
	if storedJob.Status == job.StatusCompleted {
		return redisQueue.Acknowledge(ctx, message.ID)
	}

	attempts := storedJob.Attempts + 1

	if err := jobRepository.UpdateStatus(
		ctx,
		storedJob.ID,
		job.StatusRunning,
		attempts,
	); err != nil {
		return fmt.Errorf("mark job as running: %w", err)
	}

	executionErr := jobExecutor.Execute(
		ctx,
		storedJob.ID,
		storedJob.Type,
		storedJob.Payload,
	)

	if executionErr != nil {
		if err := jobRepository.UpdateStatus(
			ctx,
			storedJob.ID,
			job.StatusFailed,
			attempts,
		); err != nil {
			return fmt.Errorf(
				"execution failed (%v) and status update failed: %w",
				executionErr,
				err,
			)
		}

		if err := redisQueue.Acknowledge(ctx, message.ID); err != nil {
			return fmt.Errorf(
				"execution failed (%v) and acknowledgement failed: %w",
				executionErr,
				err,
			)
		}

		return fmt.Errorf("execute job: %w", executionErr)
	}

	if err := jobRepository.UpdateStatus(
		ctx,
		storedJob.ID,
		job.StatusCompleted,
		attempts,
	); err != nil {
		return fmt.Errorf("mark job as completed: %w", err)
	}

	if err := redisQueue.Acknowledge(ctx, message.ID); err != nil {
		return fmt.Errorf("acknowledge message: %w", err)
	}

	return nil
}
