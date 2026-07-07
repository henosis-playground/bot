ALTER TABLE environment
    ADD COLUMN retired_at TIMESTAMPTZ;

ALTER TABLE environment_member
    ADD COLUMN retired_at TIMESTAMPTZ,
    ADD COLUMN component TEXT,
    ADD COLUMN head_branch TEXT,
    ADD COLUMN head_sha TEXT;
