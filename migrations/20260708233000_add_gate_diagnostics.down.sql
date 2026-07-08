ALTER TABLE advisory_gate_run
    DROP COLUMN IF EXISTS diagnostic;

ALTER TABLE gate_run
    DROP COLUMN IF EXISTS diagnostic;
