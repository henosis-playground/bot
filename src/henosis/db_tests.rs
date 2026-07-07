#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn inserts_henosis_gate_schema_rows(pool: sqlx::PgPool) -> sqlx::Result<()> {
    let gate_run_id = sqlx::query_scalar!(
        r#"
INSERT INTO gate_run (external_id, status)
VALUES ($1, $2)
RETURNING id
"#,
        "test-gate-run",
        "pending"
    )
    .fetch_one(&pool)
    .await?;

    let candidate_world_id = sqlx::query_scalar!(
        r#"
INSERT INTO candidate_world (gate_run_id)
VALUES ($1)
RETURNING id
"#,
        gate_run_id
    )
    .fetch_one(&pool)
    .await?;

    sqlx::query!(
        r#"
INSERT INTO candidate_world_member (candidate_world_id, repo, pr_number, head_sha)
VALUES ($1, $2, $3, $4)
"#,
        candidate_world_id,
        "henosis-playground/service-a",
        3_i64,
        "abc123"
    )
    .execute(&pool)
    .await?;

    sqlx::query!(
        r#"
INSERT INTO environment (id, lockfile_path, is_preview)
VALUES ($1, $2, $3)
"#,
        "pr-service-a-3",
        "pr-service-a-3.toml",
        true
    )
    .execute(&pool)
    .await?;

    sqlx::query!(
        r#"
INSERT INTO environment_member (environment_id, repo, pr_number)
VALUES ($1, $2, $3)
"#,
        "pr-service-a-3",
        "henosis-playground/service-a",
        3_i64
    )
    .execute(&pool)
    .await?;

    sqlx::query!(
        r#"
INSERT INTO lockfile_revision (environment_id, gate_run_id, commit_sha)
VALUES ($1, $2, $3)
"#,
        "pr-service-a-3",
        gate_run_id,
        "def456"
    )
    .execute(&pool)
    .await?;

    let exists = sqlx::query_scalar!(
        r#"
SELECT EXISTS(
    SELECT 1 FROM lockfile_revision WHERE commit_sha = $1
) AS "exists!"
"#,
        "def456"
    )
    .fetch_one(&pool)
    .await?;

    assert!(exists);
    Ok(())
}
