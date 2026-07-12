ALTER TABLE environment
ADD COLUMN display_label TEXT;

CREATE UNIQUE INDEX environment_live_display_label
ON environment (display_label)
WHERE retired_at IS NULL AND display_label IS NOT NULL;
