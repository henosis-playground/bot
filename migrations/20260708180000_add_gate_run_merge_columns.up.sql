ALTER TABLE gate_run
ADD COLUMN world JSONB,
ADD COLUMN merge_commit_sha TEXT,
ADD COLUMN dev_bump_commit_sha TEXT;
