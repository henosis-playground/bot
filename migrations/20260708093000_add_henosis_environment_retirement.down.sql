ALTER TABLE environment_member
    DROP COLUMN IF EXISTS head_sha,
    DROP COLUMN IF EXISTS head_branch,
    DROP COLUMN IF EXISTS component,
    DROP COLUMN IF EXISTS retired_at;

ALTER TABLE environment
    DROP COLUMN IF EXISTS retired_at;
