#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn inserts_henosis_gate_schema_rows(pool: sqlx::PgPool) -> sqlx::Result<()> {
    let gate_run_id: i64 = sqlx::query_scalar(
        r#"
INSERT INTO gate_run (external_id, status)
VALUES ($1, $2)
RETURNING id
"#,
    )
    .bind("test-gate-run")
    .bind("pending")
    .fetch_one(&pool)
    .await?;

    let candidate_world_id: i64 = sqlx::query_scalar(
        r#"
INSERT INTO candidate_world (gate_run_id)
VALUES ($1)
RETURNING id
"#,
    )
    .bind(gate_run_id)
    .fetch_one(&pool)
    .await?;

    sqlx::query(
        r#"
INSERT INTO candidate_world_member (candidate_world_id, repo, pr_number, head_sha)
VALUES ($1, $2, $3, $4)
"#,
    )
    .bind(candidate_world_id)
    .bind("henosis-playground/service-a")
    .bind(3_i64)
    .bind("abc123")
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"
INSERT INTO environment (id, manifest_path, is_preview)
VALUES ($1, $2, $3)
"#,
    )
    .bind("pr-service-a-3")
    .bind("pr-service-a-3.toml")
    .bind(true)
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"
INSERT INTO environment_member (environment_id, repo, pr_number)
VALUES ($1, $2, $3)
"#,
    )
    .bind("pr-service-a-3")
    .bind("henosis-playground/service-a")
    .bind(3_i64)
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"
INSERT INTO manifest_revision (environment_id, gate_run_id, commit_sha)
VALUES ($1, $2, $3)
"#,
    )
    .bind("pr-service-a-3")
    .bind(gate_run_id)
    .bind("def456")
    .execute(&pool)
    .await?;

    let exists: bool = sqlx::query_scalar(
        r#"
SELECT EXISTS(
    SELECT 1 FROM manifest_revision WHERE commit_sha = $1
) AS "exists!"
"#,
    )
    .bind("def456")
    .fetch_one(&pool)
    .await?;

    assert!(exists);
    Ok(())
}
