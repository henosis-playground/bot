DROP TABLE environment_render_comment;

ALTER TABLE environment_render
DROP COLUMN publication_url,
DROP COLUMN publication_revision,
DROP COLUMN generation;

ALTER TABLE environment
DROP COLUMN desired_render_key;
