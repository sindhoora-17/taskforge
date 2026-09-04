package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Executor struct {
	outputDirectory string
}

type generateReportPayload struct {
	ReportName string `json:"report_name"`
}

type generatedReport struct {
	JobID       string    `json:"job_id"`
	ReportName  string    `json:"report_name"`
	Status      string    `json:"status"`
	GeneratedAt time.Time `json:"generated_at"`
}

type flakyTaskPayload struct {
	FailuresBeforeSuccess int `json:"failures_before_success"`
}

type flakyTaskResult struct {
	JobID       string    `json:"job_id"`
	Status      string    `json:"status"`
	Attempts    int       `json:"attempts"`
	CompletedAt time.Time `json:"completed_at"`
}

func New(outputDirectory string) *Executor {
	return &Executor{
		outputDirectory: outputDirectory,
	}
}

func (e *Executor) Execute(
	ctx context.Context,
	jobID string,
	jobType string,
	payload json.RawMessage,
	attempt int,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	switch jobType {
	case "generate_report":
		return e.generateReport(jobID, payload)
	case "flaky_task":
		return e.executeFlakyTask(jobID, payload, attempt)
	default:
		return fmt.Errorf("unsupported job type %q", jobType)
	}
}

func (e *Executor) generateReport(
	jobID string,
	payload json.RawMessage,
) error {
	var request generateReportPayload

	if err := json.Unmarshal(payload, &request); err != nil {
		return fmt.Errorf("decode generate_report payload: %w", err)
	}

	request.ReportName = strings.TrimSpace(request.ReportName)
	if request.ReportName == "" {
		return errors.New("report_name is required")
	}

	report := generatedReport{
		JobID:       jobID,
		ReportName:  request.ReportName,
		Status:      "generated",
		GeneratedAt: time.Now().UTC(),
	}

	return e.writeResult(jobID, report)
}

func (e *Executor) executeFlakyTask(
	jobID string,
	payload json.RawMessage,
	attempt int,
) error {
	var request flakyTaskPayload

	if err := json.Unmarshal(payload, &request); err != nil {
		return fmt.Errorf("decode flaky_task payload: %w", err)
	}

	if request.FailuresBeforeSuccess < 0 {
		return errors.New("failures_before_success cannot be negative")
	}

	if attempt <= request.FailuresBeforeSuccess {
		return fmt.Errorf(
			"simulated transient failure on attempt %d",
			attempt,
		)
	}

	result := flakyTaskResult{
		JobID:       jobID,
		Status:      "completed_after_retries",
		Attempts:    attempt,
		CompletedAt: time.Now().UTC(),
	}

	return e.writeResult(jobID, result)
}

func (e *Executor) writeResult(
	jobID string,
	result any,
) error {
	if err := os.MkdirAll(e.outputDirectory, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("encode result: %w", err)
	}

	data = append(data, '\n')

	filename := filepath.Join(e.outputDirectory, jobID+".json")

	if err := os.WriteFile(filename, data, 0o644); err != nil {
		return fmt.Errorf("write result: %w", err)
	}

	return nil
}
