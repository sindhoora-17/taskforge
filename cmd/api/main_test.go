package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sindhoora-17/taskforge/internal/job"
)

type fakeJobRepository struct {
	jobs map[string]job.Job
}

type fakeJobQueue struct {
	jobs []job.Job
}

func (f *fakeJobQueue) Enqueue(
	_ context.Context,
	newJob job.Job,
) (string, error) {
	f.jobs = append(f.jobs, newJob)
	return "1-0", nil
}

func (f *fakeJobRepository) Create(
	_ context.Context,
	newJob job.Job,
) error {
	f.jobs[newJob.ID] = newJob
	return nil
}

func (f *fakeJobRepository) GetByID(
	_ context.Context,
	jobID string,
) (job.Job, error) {
	return f.jobs[jobID], nil
}

func TestCreateJobHandler(t *testing.T) {
	repository := &fakeJobRepository{
		jobs: make(map[string]job.Job),
	}

	handler := &api{
		jobs: repository,
	}

	body := strings.NewReader(`{
		"type": "generate_report",
		"payload": {
			"report_name": "monthly-sales"
		}
	}`)

	request := httptest.NewRequest(http.MethodPost, "/jobs", body)
	response := httptest.NewRecorder()

	handler.createJobHandler(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusAccepted,
			response.Code,
		)
	}

	var createdJob job.Job

	if err := json.NewDecoder(response.Body).Decode(&createdJob); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if createdJob.ID == "" {
		t.Error("expected job ID to be generated")
	}

	if createdJob.Type != "generate_report" {
		t.Errorf(
			"expected type generate_report, got %s",
			createdJob.Type,
		)
	}

	if createdJob.Status != job.StatusQueued {
		t.Errorf(
			"expected status %s, got %s",
			job.StatusQueued,
			createdJob.Status,
		)
	}

	if createdJob.MaxAttempts != 3 {
		t.Errorf(
			"expected default max attempts to be 3, got %d",
			createdJob.MaxAttempts,
		)
	}

	if createdJob.TimeoutSeconds != 30 {
		t.Errorf(
			"expected default timeout to be 30 seconds, got %d",
			createdJob.TimeoutSeconds,
		)
	}

	if _, exists := repository.jobs[createdJob.ID]; !exists {
		t.Error("expected created job to be stored")
	}
}

func TestCreateJobHandlerRejectsMissingType(t *testing.T) {
	repository := &fakeJobRepository{
		jobs: make(map[string]job.Job),
	}

	handler := &api{
		jobs: repository,
	}

	body := strings.NewReader(`{
		"payload": {
			"report_name": "monthly-sales"
		}
	}`)

	request := httptest.NewRequest(http.MethodPost, "/jobs", body)
	response := httptest.NewRecorder()

	handler.createJobHandler(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusBadRequest,
			response.Code,
		)
	}

	if len(repository.jobs) != 0 {
		t.Error("invalid job should not be stored")
	}
}

func TestCreateJobHandlerRejectsInvalidTimeout(t *testing.T) {
	repository := &fakeJobRepository{
		jobs: make(map[string]job.Job),
	}

	handler := &api{
		jobs: repository,
	}

	body := strings.NewReader(`{
		"type": "generate_report",
		"payload": {
			"report_name": "monthly-sales"
		},
		"timeout_seconds": 3601
	}`)

	request := httptest.NewRequest(http.MethodPost, "/jobs", body)
	response := httptest.NewRecorder()

	handler.createJobHandler(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusBadRequest,
			response.Code,
		)
	}

	if len(repository.jobs) != 0 {
		t.Error("job with invalid timeout should not be stored")
	}
}

func (f *fakeJobRepository) GetMetrics(
	_ context.Context,
) (job.Metrics, error) {
	var metrics job.Metrics

	for _, storedJob := range f.jobs {
		metrics.Total++

		switch storedJob.Status {
		case job.StatusQueued:
			metrics.Queued++
		case job.StatusRunning:
			metrics.Running++
		case job.StatusRetrying:
			metrics.Retrying++
		case job.StatusCompleted:
			metrics.Completed++
		case job.StatusFailed:
			metrics.Failed++
		}
	}

	return metrics, nil
}

func TestMetricsHandler(t *testing.T) {
	repository := &fakeJobRepository{
		jobs: map[string]job.Job{
			"job-1": {
				ID:     "job-1",
				Status: job.StatusCompleted,
			},
			"job-2": {
				ID:     "job-2",
				Status: job.StatusCompleted,
			},
			"job-3": {
				ID:     "job-3",
				Status: job.StatusFailed,
			},
		},
	}

	handler := &api{
		jobs: repository,
	}

	request := httptest.NewRequest(
		http.MethodGet,
		"/metrics",
		nil,
	)
	response := httptest.NewRecorder()

	handler.metricsHandler(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusOK,
			response.Code,
		)
	}

	var metrics job.Metrics

	if err := json.NewDecoder(response.Body).Decode(&metrics); err != nil {
		t.Fatalf("failed to decode metrics response: %v", err)
	}

	if metrics.Total != 3 {
		t.Errorf("expected 3 total jobs, got %d", metrics.Total)
	}

	if metrics.Completed != 2 {
		t.Errorf(
			"expected 2 completed jobs, got %d",
			metrics.Completed,
		)
	}

	if metrics.Failed != 1 {
		t.Errorf(
			"expected 1 failed job, got %d",
			metrics.Failed,
		)
	}
}
