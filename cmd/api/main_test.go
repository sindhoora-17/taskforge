package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sindhoora-17/taskforge/internal/job"
)

func TestCreateJobHandler(t *testing.T) {
	store := &jobStore{
		jobs: make(map[string]job.Job),
	}

	body := strings.NewReader(`{
		"type": "generate_report",
		"payload": {
			"report_name": "monthly-sales"
		}
	}`)

	request := httptest.NewRequest(http.MethodPost, "/jobs", body)
	response := httptest.NewRecorder()

	store.createJobHandler(response, request)

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

	if _, exists := store.jobs[createdJob.ID]; !exists {
		t.Error("expected created job to be stored")
	}
}

func TestCreateJobHandlerRejectsMissingType(t *testing.T) {
	store := &jobStore{
		jobs: make(map[string]job.Job),
	}

	body := strings.NewReader(`{
		"payload": {
			"report_name": "monthly-sales"
		}
	}`)

	request := httptest.NewRequest(http.MethodPost, "/jobs", body)
	response := httptest.NewRecorder()

	store.createJobHandler(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf(
			"expected status %d, got %d",
			http.StatusBadRequest,
			response.Code,
		)
	}

	if len(store.jobs) != 0 {
		t.Error("invalid job should not be stored")
	}
}
