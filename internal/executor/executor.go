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
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	switch jobType {
	case "generate_report":
		return e.generateReport(jobID, payload)
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

	if err := os.MkdirAll(e.outputDirectory, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	report := generatedReport{
		JobID:       jobID,
		ReportName:  request.ReportName,
		Status:      "generated",
		GeneratedAt: time.Now().UTC(),
	}

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode generated report: %w", err)
	}

	data = append(data, '\n')

	filename := filepath.Join(e.outputDirectory, jobID+".json")

	if err := os.WriteFile(filename, data, 0o644); err != nil {
		return fmt.Errorf("write generated report: %w", err)
	}

	return nil
}
