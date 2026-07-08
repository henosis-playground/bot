INSERT INTO manifest_revision (environment_id, gate_run_id, commit_sha)
SELECT 'dev', id, 'manifest-rename-sample'
FROM gate_run
WHERE external_id = 'sample-gate-run';
