package outbox

import (
	"testing"
	"time"
)

func TestCalculatePublishRetryDelay(t *testing.T) {
	tests := []struct {
		name     string
		attempts int
		expected time.Duration
	}{
		{
			name:     "zero attempts uses base delay",
			attempts: 0,
			expected: time.Second,
		},
		{
			name:     "first attempt",
			attempts: 1,
			expected: time.Second,
		},
		{
			name:     "second attempt",
			attempts: 2,
			expected: 2 * time.Second,
		},
		{
			name:     "third attempt",
			attempts: 3,
			expected: 4 * time.Second,
		},
		{
			name:     "sixth attempt",
			attempts: 6,
			expected: 32 * time.Second,
		},
		{
			name:     "seventh attempt reaches maximum",
			attempts: 7,
			expected: time.Minute,
		},
		{
			name:     "large attempt remains capped",
			attempts: 50,
			expected: time.Minute,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := calculatePublishRetryDelay(
				test.attempts,
			)

			if actual != test.expected {
				t.Fatalf(
					"expected %s, got %s",
					test.expected,
					actual,
				)
			}
		})
	}
}
