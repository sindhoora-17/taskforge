DROP INDEX IF EXISTS idx_job_outbox_unpublished;

CREATE INDEX idx_job_outbox_unpublished
    ON job_outbox (created_at)
    WHERE published_at IS NULL;

ALTER TABLE job_outbox
DROP COLUMN IF EXISTS next_attempt_at;