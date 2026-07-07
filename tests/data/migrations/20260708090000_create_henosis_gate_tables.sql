INSERT INTO gate_run (external_id, status)
VALUES ('sample-gate-run', 'passed');

INSERT INTO candidate_world (gate_run_id)
SELECT id FROM gate_run WHERE external_id = 'sample-gate-run';

INSERT INTO candidate_world_member (candidate_world_id, repo, pr_number, head_sha)
SELECT id, 'henosis-playground/service-a', 1, 'abc123'
FROM candidate_world
WHERE gate_run_id = (SELECT id FROM gate_run WHERE external_id = 'sample-gate-run');

INSERT INTO environment (id, lockfile_path, is_preview)
VALUES ('dev', 'dev.toml', FALSE);

INSERT INTO environment_member (environment_id, repo, pr_number)
VALUES ('dev', 'henosis-playground/service-a', 1);

INSERT INTO lockfile_revision (environment_id, gate_run_id, commit_sha)
SELECT 'dev', id, 'def456'
FROM gate_run
WHERE external_id = 'sample-gate-run';
