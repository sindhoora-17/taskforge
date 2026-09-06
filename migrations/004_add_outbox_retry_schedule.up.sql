ALTER TABLE job_outbox
ADD COLUMN next_attempt_at TIMESTAMPTZ;

UPDATE job_outbox
SET next_attempt_at = NOW()
WHERE published_at IS NULL;

ALTER TABLE job_outbox
ALTER COLUMN next_attempt_at SET DEFAULT NOW();

DROP INDEX IF EXISTS idx_job_outbox_unpublished;

CREATE INDEX idx_job_outbox_unpublished
    ON job_outbox (next_attempt_at, created_at)
    WHERE published_at IS NULL;