CREATE TABLE advisory_gate_run (
    id BIGSERIAL PRIMARY KEY,
    repo TEXT NOT NULL,
    pr_number BIGINT NOT NULL,
    head_sha TEXT NOT NULL,
    external_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX advisory_gate_run_pr_idx
ON advisory_gate_run (repo, pr_number, created_at DESC, id DESC);
