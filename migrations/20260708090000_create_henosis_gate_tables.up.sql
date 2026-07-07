CREATE TABLE gate_run (
    id BIGSERIAL PRIMARY KEY,
    external_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE candidate_world (
    id BIGSERIAL PRIMARY KEY,
    gate_run_id BIGINT NOT NULL REFERENCES gate_run(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE candidate_world_member (
    id BIGSERIAL PRIMARY KEY,
    candidate_world_id BIGINT NOT NULL REFERENCES candidate_world(id) ON DELETE CASCADE,
    repo TEXT NOT NULL,
    pr_number BIGINT NOT NULL,
    head_sha TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE environment (
    id TEXT PRIMARY KEY,
    lockfile_path TEXT NOT NULL,
    is_preview BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE environment_member (
    id BIGSERIAL PRIMARY KEY,
    environment_id TEXT NOT NULL REFERENCES environment(id) ON DELETE CASCADE,
    repo TEXT NOT NULL,
    pr_number BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(environment_id, repo, pr_number)
);

CREATE TABLE lockfile_revision (
    id BIGSERIAL PRIMARY KEY,
    environment_id TEXT NOT NULL REFERENCES environment(id),
    gate_run_id BIGINT REFERENCES gate_run(id),
    commit_sha TEXT NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
