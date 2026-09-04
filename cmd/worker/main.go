package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/sindhoora-17/taskforge/internal/executor"
	"github.com/sindhoora-17/taskforge/internal/queue"
	"github.com/sindhoora-17/taskforge/internal/repository"
	"github.com/sindhoora-17/taskforge/internal/worker"
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
	workerConcurrency := 4

	if configuredConcurrency := os.Getenv("WORKER_CONCURRENCY"); configuredConcurrency != "" {
		parsedConcurrency, err := strconv.Atoi(configuredConcurrency)
		if err != nil || parsedConcurrency < 1 {
			log.Fatal("WORKER_CONCURRENCY must be a positive integer")
		}

		workerConcurrency = parsedConcurrency
	}

	jobRepository := repository.NewPostgresJobRepository(pool)
	jobExecutor := executor.New("output")

	messageProcessor := worker.NewProcessor(
		jobRepository,
		redisQueue,
		jobExecutor,
	)
	workerPool := worker.NewPool(
		redisQueue,
		messageProcessor,
		consumerName,
		workerConcurrency,
	)

	log.Printf(
		"Worker process %s started with concurrency %d",
		consumerName,
		workerConcurrency,
	)

	workerPool.Run(ctx)

	log.Printf("Worker process %s stopped", consumerName)
}
