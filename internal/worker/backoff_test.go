package worker

import (
	"testing"
	"time"
)

func TestCalculateDependencyBackoff(t *testing.T) {
	tests := []struct {
		failures int
		expected time.Duration
	}{
		{failures: 0, expected: time.Second},
		{failures: 1, expected: time.Second},
		{failures: 2, expected: 2 * time.Second},
		{failures: 3, expected: 4 * time.Second},
		{failures: 4, expected: 8 * time.Second},
		{failures: 5, expected: 16 * time.Second},
		{failures: 6, expected: 30 * time.Second},
		{failures: 20, expected: 30 * time.Second},
	}

	for _, test := range tests {
		actual := calculateDependencyBackoff(test.failures)

		if actual != test.expected {
			t.Errorf(
				"failures %d: expected %s, got %s",
				test.failures,
				test.expected,
				actual,
			)
		}
	}
}
