UPDATE environment
SET desired_render_key = 'generation:1'
WHERE id = 'dev';

UPDATE environment_render
SET generation = 1,
    publication_revision = 'sample-publication-sha',
    publication_url = 'https://github.com/henosis-playground/deploy/commit/sample-publication-sha'
WHERE environment_id = 'dev';

INSERT INTO environment_render_comment (
    environment_id,
    generation,
    consumer,
    repo,
    pr_number,
    node_id,
    original_body
)
VALUES (
    'dev',
    1,
    'service-a',
    'henosis-playground/service-a',
    1,
    'sample-render-comment-node',
    'sample generation diagnostic'
);
