BEGIN;

UPDATE jobs
SET status = 'failed'
WHERE status = 'retrying';

ALTER TABLE jobs
    DROP CONSTRAINT jobs_status_check;

ALTER TABLE jobs
    ADD CONSTRAINT jobs_status_check
    CHECK (
        status IN (
            'queued',
            'running',
            'completed',
            'failed'
        )
    );

ALTER TABLE jobs
    DROP COLUMN next_attempt_at,
    DROP COLUMN last_error;

COMMIT;