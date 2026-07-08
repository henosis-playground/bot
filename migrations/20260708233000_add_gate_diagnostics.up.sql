ALTER TABLE gate_run
    ADD COLUMN diagnostic TEXT;

ALTER TABLE advisory_gate_run
    ADD COLUMN diagnostic TEXT;
