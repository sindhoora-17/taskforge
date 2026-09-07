package worker

import "time"

const (
	initialDependencyBackoff = time.Second
	maxDependencyBackoff     = 30 * time.Second
)

func calculateDependencyBackoff(
	consecutiveFailures int,
) time.Duration {
	if consecutiveFailures <= 1 {
		return initialDependencyBackoff
	}

	delay := initialDependencyBackoff

	for failure := 1; failure < consecutiveFailures; failure++ {
		if delay >= maxDependencyBackoff/2 {
			return maxDependencyBackoff
		}

		delay *= 2
	}

	if delay > maxDependencyBackoff {
		return maxDependencyBackoff
	}

	return delay
}
