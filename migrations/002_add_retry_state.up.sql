BEGIN;

ALTER TABLE jobs
    ADD COLUMN last_error TEXT,
    ADD COLUMN next_attempt_at TIMESTAMPTZ;

ALTER TABLE jobs
    DROP CONSTRAINT jobs_status_check;

ALTER TABLE jobs
    ADD CONSTRAINT jobs_status_check
    CHECK (
        status IN (
            'queued',
            'running',
            'retrying',
            'completed',
            'failed'
        )
    );

COMMIT;