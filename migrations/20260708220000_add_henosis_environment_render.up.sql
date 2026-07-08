CREATE TABLE environment_render (
    id BIGSERIAL PRIMARY KEY,
    environment_id TEXT NOT NULL,
    commit_sha TEXT NOT NULL,
    status TEXT NOT NULL,
    run_url TEXT NOT NULL,
    excerpt TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX environment_render_environment_id_created_at_idx
    ON environment_render(environment_id, created_at DESC, id DESC);

CREATE INDEX environment_render_commit_sha_idx
    ON environment_render(commit_sha);
