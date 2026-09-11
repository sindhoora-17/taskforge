ALTER TABLE jobs
DROP CONSTRAINT IF EXISTS jobs_timeout_seconds_check;

ALTER TABLE jobs
DROP COLUMN IF EXISTS timeout_seconds;