ALTER TABLE environment
    RENAME COLUMN lockfile_path TO manifest_path;

ALTER TABLE lockfile_revision
    RENAME TO manifest_revision;
