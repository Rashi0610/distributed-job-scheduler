-- Jobs: what the user registered. One row per job definition.
CREATE TABLE IF NOT EXISTS jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    target_url      TEXT NOT NULL,
    payload         JSONB DEFAULT '{}',

    -- Exactly one of these two is set, depending on job type.
    cron_expression TEXT,              -- e.g. "*/5 * * * *" for recurring jobs
    run_at          TIMESTAMPTZ,       -- for one-time jobs

    timezone        TEXT NOT NULL DEFAULT 'UTC',
    status          TEXT NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'paused', 'archived')),

    -- The scheduler's poll query filters on this. Indexed below.
    next_run_at     TIMESTAMPTZ NOT NULL,

    max_attempts    INT NOT NULL DEFAULT 3,
    timeout_ms      INT NOT NULL DEFAULT 10000,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT one_schedule_type CHECK (
        (cron_expression IS NOT NULL AND run_at IS NULL) OR
        (cron_expression IS NULL AND run_at IS NOT NULL)
    )
);

-- This index is what makes the scheduler's poll query fast even with
-- millions of jobs: it only scans active jobs ordered by due time.
CREATE INDEX IF NOT EXISTS idx_jobs_due
    ON jobs (next_run_at)
    WHERE status = 'active';

-- Executions: the audit trail. One row per attempt to run a job.
-- This is what your load test will query to prove exactly-once execution.
CREATE TABLE IF NOT EXISTS executions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,

    scheduled_for   TIMESTAMPTZ NOT NULL,
    attempt         INT NOT NULL DEFAULT 1,

    status          TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'running', 'success', 'failed', 'timeout', 'dead_letter')),

    -- Which worker claimed this execution. Useful for debugging races.
    claimed_by      TEXT,

    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,

    response_status INT,
    response_body   TEXT,
    error_message   TEXT,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_executions_job_id ON executions (job_id);
CREATE INDEX IF NOT EXISTS idx_executions_status ON executions (status);

-- Needed for gen_random_uuid() above.
CREATE EXTENSION IF NOT EXISTS pgcrypto;
