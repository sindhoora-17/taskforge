ALTER TABLE jobs
ADD COLUMN timeout_seconds INTEGER NOT NULL DEFAULT 30;

ALTER TABLE jobs
ADD CONSTRAINT jobs_timeout_seconds_check
CHECK (timeout_seconds >= 1 AND timeout_seconds <= 3600);