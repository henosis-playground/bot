use crate::henosis::db::{PgEnvironmentStore, PgQueueStore};
use crate::henosis::environment::EnvironmentStore;
use crate::henosis::queue::{
    CandidateComponent, CandidateWorld, INVALIDATED_STATUS, PENDING_STATUS, QueuePullRequest,
    QueueStore, gate_external_id,
};

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

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn record_gate_run_deletes_invalidated_duplicate_external_id(
    pool: sqlx::PgPool,
) -> anyhow::Result<()> {
    let world = CandidateWorld {
        members: vec![QueuePullRequest::new(
            "henosis-playground/service-a",
            3,
            "service-a",
            "abc123",
            "pr/3",
            "abc123",
        )],
        components: vec![CandidateComponent {
            name: "service-a".to_string(),
            repo: "henosis-playground/service-a".to_string(),
            r#ref: "abc123".to_string(),
            digest: "sha256:abc123".to_string(),
            candidate: true,
        }],
    };
    let external_id = gate_external_id(&world)?;

    sqlx::query(
        r#"
INSERT INTO gate_run (external_id, status, world)
VALUES ($1, $2, $3::jsonb)
"#,
    )
    .bind(&external_id)
    .bind(INVALIDATED_STATUS)
    .bind(serde_json::to_string(&world)?)
    .execute(&pool)
    .await?;

    let mut store = PgQueueStore::new(pool.clone());
    let run = store.record_gate_run(&world).await?;

    assert_eq!(run.external_id, external_id);
    assert_eq!(run.status, PENDING_STATUS);
    let rows: i64 = sqlx::query_scalar(
        r#"
SELECT COUNT(*)
FROM gate_run
WHERE external_id = $1
"#,
    )
    .bind(&external_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(rows, 1);

    let status: String = sqlx::query_scalar(
        r#"
SELECT status
FROM gate_run
WHERE external_id = $1
"#,
    )
    .bind(&external_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(status, PENDING_STATUS);
    Ok(())
}

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn named_preview_environment_can_be_recreated_after_retirement(
    pool: sqlx::PgPool,
) -> anyhow::Result<()> {
    let mut store = PgEnvironmentStore::new(pool.clone());
    let named_id = "preview-demo-shared";
    store
        .upsert_environment(named_id, "preview-demo-shared.toml", true)
        .await?;
    store.retire_environment(named_id).await?;
    sqlx::query("INSERT INTO manifest_revision (environment_id, commit_sha) VALUES ($1, $2)")
        .bind(named_id)
        .bind("old-commit")
        .execute(&pool)
        .await?;
    sqlx::query(
        "INSERT INTO environment_render (environment_id, commit_sha, status, run_url) VALUES ($1, $2, $3, $4)",
    )
    .bind(named_id)
    .bind("old-commit")
    .bind("failure")
    .bind("https://example.test/run")
    .execute(&pool)
    .await?;

    store
        .upsert_environment(named_id, "preview-demo-shared.toml", true)
        .await?;

    assert!(store.active_environment(named_id).await?.is_some());
    let revision_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM manifest_revision WHERE environment_id = $1")
            .bind(named_id)
            .fetch_one(&pool)
            .await?;
    let render_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM environment_render WHERE environment_id = $1")
            .bind(named_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(revision_count, 0);
    assert_eq!(render_count, 0);

    let uuid_id = "preview-00000000-0000-4000-8000-000000000001";
    store
        .upsert_environment(
            uuid_id,
            "preview-00000000-0000-4000-8000-000000000001.toml",
            true,
        )
        .await?;
    store.retire_environment(uuid_id).await?;
    let error = store
        .upsert_environment(
            uuid_id,
            "preview-00000000-0000-4000-8000-000000000001.toml",
            true,
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Cannot reuse retired environment id")
    );

    Ok(())
}
