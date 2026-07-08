ALTER TABLE manifest_revision
    RENAME TO lockfile_revision;

ALTER TABLE environment
    RENAME COLUMN manifest_path TO lockfile_path;
