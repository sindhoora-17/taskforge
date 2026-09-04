package executor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestExecuteGenerateReport(t *testing.T) {
	outputDirectory := t.TempDir()
	jobExecutor := New(outputDirectory)

	payload := json.RawMessage(`{
		"report_name": "weekly-report"
	}`)

	err := jobExecutor.Execute(
		context.Background(),
		"job-123",
		"generate_report",
		payload,
		1,
	)
	if err != nil {
		t.Fatalf("expected job to succeed, got %v", err)
	}

	filename := filepath.Join(outputDirectory, "job-123.json")

	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("expected report file to exist: %v", err)
	}

	var report generatedReport

	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatalf("failed to decode generated report: %v", err)
	}

	if report.JobID != "job-123" {
		t.Errorf("expected job ID job-123, got %s", report.JobID)
	}

	if report.ReportName != "weekly-report" {
		t.Errorf(
			"expected report name weekly-report, got %s",
			report.ReportName,
		)
	}

	if report.Status != "generated" {
		t.Errorf(
			"expected status generated, got %s",
			report.Status,
		)
	}

	if report.GeneratedAt.IsZero() {
		t.Error("expected generated_at to be set")
	}
}

func TestExecuteRejectsMissingReportName(t *testing.T) {
	jobExecutor := New(t.TempDir())

	payload := json.RawMessage(`{
		"report_name": " "
	}`)

	err := jobExecutor.Execute(
		context.Background(),
		"job-123",
		"generate_report",
		payload,
		1,
	)

	if err == nil {
		t.Fatal("expected missing report name to return an error")
	}
}

func TestExecuteRejectsUnsupportedJobType(t *testing.T) {
	jobExecutor := New(t.TempDir())

	err := jobExecutor.Execute(
		context.Background(),
		"job-123",
		"unknown_job",
		json.RawMessage(`{}`),
		1,
	)

	if err == nil {
		t.Fatal("expected unsupported job type to return an error")
	}
}
