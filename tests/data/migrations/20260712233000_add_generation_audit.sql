ALTER TABLE environment
ADD COLUMN desired_render_key TEXT;

ALTER TABLE environment_render
ADD COLUMN generation BIGINT,
ADD COLUMN publication_revision TEXT,
ADD COLUMN publication_url TEXT;

CREATE TABLE environment_render_comment (
    environment_id TEXT NOT NULL,
    generation BIGINT NOT NULL,
    consumer TEXT NOT NULL,
    repo TEXT NOT NULL,
    pr_number BIGINT NOT NULL,
    node_id TEXT NOT NULL,
    original_body TEXT NOT NULL,
    resolved_generation BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (environment_id, generation, consumer, repo, pr_number)
);
