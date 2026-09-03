package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sindhoora-17/taskforge/internal/job"
)

type jobStore struct {
	mu   sync.RWMutex
	jobs map[string]job.Job
}

func main() {
	store := &jobStore{
		jobs: make(map[string]job.Job),
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("POST /jobs", store.createJobHandler)
	mux.HandleFunc("GET /jobs/{id}", store.getJobHandler)

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

func (s *jobStore) createJobHandler(w http.ResponseWriter, r *http.Request) {
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

	s.mu.Lock()
	s.jobs[newJob.ID] = newJob
	s.mu.Unlock()

	writeJSON(w, http.StatusAccepted, newJob)
}

func (s *jobStore) getJobHandler(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")

	s.mu.RLock()
	storedJob, exists := s.jobs[jobID]
	s.mu.RUnlock()

	if !exists {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": "job not found",
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
