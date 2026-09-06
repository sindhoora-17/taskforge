package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/sindhoora-17/taskforge/internal/job"
	"github.com/sindhoora-17/taskforge/internal/outbox"
	taskqueue "github.com/sindhoora-17/taskforge/internal/queue"
	"github.com/sindhoora-17/taskforge/internal/repository"
)

type jobRepository interface {
	Create(ctx context.Context, newJob job.Job) error
	GetByID(ctx context.Context, jobID string) (job.Job, error)
}

type api struct {
	jobs jobRepository
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println(".env file not found; using system environment variables")
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("failed to create database pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("failed to connect to PostgreSQL: %v", err)
	}

	log.Println("Connected to PostgreSQL")
	redisAddress := os.Getenv("REDIS_ADDR")
	if redisAddress == "" {
		log.Fatal("REDIS_ADDR is required")
	}

	redisQueue := taskqueue.NewRedisQueue(redisAddress)

	defer func() {
		if err := redisQueue.Close(); err != nil {
			log.Printf("failed to close Redis connection: %v", err)
		}
	}()

	if err := redisQueue.Ping(ctx); err != nil {
		log.Fatalf("failed to connect to Redis: %v", err)
	}

	log.Println("Connected to Redis")

	jobRepository := repository.NewPostgresJobRepository(pool)

	handler := &api{
		jobs: jobRepository,
	}

	outboxDispatcher := outbox.NewDispatcher(
		pool,
		redisQueue,
		250*time.Millisecond,
	)

	go outboxDispatcher.Run(ctx)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("POST /jobs", handler.createJobHandler)
	mux.HandleFunc("GET /jobs/{id}", handler.getJobHandler)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Println("TaskForge API is running on http://localhost:8080")

	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "healthy",
		"service": "taskforge-api",
	})
}

func (a *api) createJobHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var request job.CreateRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&request); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "request body must contain valid JSON",
		})
		return
	}

	request.Type = strings.TrimSpace(request.Type)

	if request.Type == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "job type is required",
		})
		return
	}

	if len(request.Payload) == 0 || string(request.Payload) == "null" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "job payload is required",
		})
		return
	}

	if request.MaxAttempts == 0 {
		request.MaxAttempts = 3
	}

	if request.MaxAttempts < 1 || request.MaxAttempts > 10 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "max_attempts must be between 1 and 10",
		})
		return
	}

	now := time.Now().UTC()

	newJob := job.Job{
		ID:          uuid.NewString(),
		Type:        request.Type,
		Payload:     request.Payload,
		Status:      job.StatusQueued,
		Attempts:    0,
		MaxAttempts: request.MaxAttempts,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if err := a.jobs.Create(r.Context(), newJob); err != nil {
		log.Printf("failed to store job: %v", err)

		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to create job",
		})
		return
	}

	log.Printf(
		"Created job %s with a pending outbox event",
		newJob.ID,
	)

	writeJSON(w, http.StatusAccepted, newJob)
}

func (a *api) getJobHandler(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")

	if _, err := uuid.Parse(jobID); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid job id",
		})
		return
	}

	storedJob, err := a.jobs.GetByID(r.Context(), jobID)

	if errors.Is(err, repository.ErrJobNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": "job not found",
		})
		return
	}

	if err != nil {
		log.Printf("failed to retrieve job: %v", err)

		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to retrieve job",
		})
		return
	}

	writeJSON(w, http.StatusOK, storedJob)
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("failed to encode response: %v", err)
	}
}
