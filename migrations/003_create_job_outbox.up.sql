CREATE TABLE job_outbox (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL UNIQUE
        REFERENCES jobs(id)
        ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,

    CONSTRAINT job_outbox_publish_attempts_check
        CHECK (publish_attempts >= 0)
);

CREATE INDEX idx_job_outbox_unpublished
    ON job_outbox (created_at)
    WHERE published_at IS NULL;