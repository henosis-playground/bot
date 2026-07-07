UPDATE gate_run
SET world = '{
        "members": [
            {
                "key": {
                    "repo": "henosis-playground/service-a",
                    "number": 1
                },
                "component": "service-a",
                "head_sha": "abc123",
                "head_branch": "pr/1",
                "approved_sha": "abc123"
            }
        ],
        "components": [
            {
                "name": "service-a",
                "repo": "henosis-playground/service-a",
                "ref": "abc123",
                "digest": "sha256:sample",
                "candidate": true
            }
        ]
    }'::jsonb,
    merge_commit_sha = 'sample-merge-sha',
    dev_bump_commit_sha = 'sample-dev-bump-sha'
WHERE external_id = 'sample-gate-run';
