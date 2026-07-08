UPDATE gate_run
SET diagnostic = 'sample final gate diagnostic'
WHERE external_id = 'sample-gate-run';

UPDATE advisory_gate_run
SET diagnostic = 'sample advisory gate diagnostic'
WHERE external_id = 'gate-henosis-playground-service-a-1-advisory-abc123';
