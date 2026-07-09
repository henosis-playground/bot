use anyhow::Context;
use sqlx::pool::PoolConnection;
use sqlx::{PgPool, Postgres, Row};

use crate::henosis::config::{ComponentMode, RegisteredComponent};
use crate::henosis::environment::{
    EnvironmentState, EnvironmentStore, PreviewPullRequest, PullRequestKey, RenderOutcome,
    RenderStatus, is_preview_environment_id,
};
use crate::henosis::merge::MergeStore;
use crate::henosis::queue::{
    AdvisoryGateStore, BUMPING_DEV_STATUS, CandidateWorld, GATE_PASSED_STATUS,
    GLOBAL_QUEUE_LOCK_KEY, GateStatus, INVALIDATED_STATUS, MERGING_PR_STATUS,
    PENDING_EXECUTOR_STATUS, PENDING_STATUS, QueuePullRequest, QueueStore, RUNNING_STATUS,
    RecordedGateRun, gate_external_id,
};

pub struct PgEnvironmentStore {
    pool: PgPool,
}

impl PgEnvironmentStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn active_preview_environments_for_commit_sha(
        &self,
        commit_sha: &str,
    ) -> anyhow::Result<Vec<EnvironmentState>> {
        let rows = sqlx::query(
            r#"
SELECT DISTINCT e.id, e.manifest_path, e.is_preview
FROM manifest_revision AS mr
JOIN environment AS e ON e.id = mr.environment_id
WHERE mr.commit_sha = $1
  AND e.retired_at IS NULL
  AND e.is_preview = TRUE
ORDER BY e.id
"#,
        )
        .bind(commit_sha)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(EnvironmentState {
                    id: row.try_get("id")?,
                    manifest_path: row.try_get("manifest_path")?,
                    is_preview: row.try_get("is_preview")?,
                })
            })
            .collect()
    }

    pub async fn record_render_outcome(&self, outcome: &RenderOutcome) -> anyhow::Result<()> {
        sqlx::query(
            r#"
INSERT INTO environment_render (
    environment_id,
    commit_sha,
    status,
    run_url,
    excerpt
)
VALUES ($1, $2, $3, $4, $5)
"#,
        )
        .bind(&outcome.environment_id)
        .bind(&outcome.commit_sha)
        .bind(outcome.status.as_str())
        .bind(&outcome.run_url)
        .bind(&outcome.excerpt)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn latest_render_outcome(
        &self,
        environment_id: &str,
    ) -> anyhow::Result<Option<RenderOutcome>> {
        let row = sqlx::query(
            r#"
SELECT environment_id, commit_sha, status, run_url, excerpt
FROM environment_render
WHERE environment_id = $1
ORDER BY created_at DESC, id DESC
LIMIT 1
"#,
        )
        .bind(environment_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            let status: String = row.try_get("status")?;
            Ok(RenderOutcome {
                environment_id: row.try_get("environment_id")?,
                commit_sha: row.try_get("commit_sha")?,
                status: RenderStatus::try_from(status.as_str())?,
                run_url: row.try_get("run_url")?,
                excerpt: row.try_get("excerpt")?,
            })
        })
        .transpose()
    }
}

impl EnvironmentStore for PgEnvironmentStore {
    async fn upsert_environment(
        &mut self,
        id: &str,
        manifest_path: &str,
        is_preview: bool,
    ) -> anyhow::Result<()> {
        let can_reuse_retired = can_reuse_retired_environment_id(id, is_preview);
        sqlx::query(
            r#"
INSERT INTO environment (id, manifest_path, is_preview, retired_at, updated_at)
VALUES ($1, $2, $3, NULL, NOW())
ON CONFLICT (id)
DO UPDATE SET
    manifest_path = EXCLUDED.manifest_path,
    is_preview = EXCLUDED.is_preview,
    retired_at = NULL,
    updated_at = NOW()
WHERE environment.retired_at IS NULL OR $4
RETURNING id
"#,
        )
        .bind(id)
        .bind(manifest_path)
        .bind(is_preview)
        .bind(can_reuse_retired)
        .fetch_optional(&self.pool)
        .await?
        .with_context(|| format!("Cannot reuse retired environment id `{id}`"))?;
        if can_reuse_retired {
            sqlx::query("DELETE FROM manifest_revision WHERE environment_id = $1")
                .bind(id)
                .execute(&self.pool)
                .await?;
            sqlx::query("DELETE FROM environment_render WHERE environment_id = $1")
                .bind(id)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    async fn retire_environment(&mut self, id: &str) -> anyhow::Result<()> {
        sqlx::query(
            r#"
UPDATE environment
SET retired_at = NOW(), updated_at = NOW()
WHERE id = $1
"#,
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn put_member(
        &mut self,
        environment_id: &str,
        member: &PreviewPullRequest,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
INSERT INTO environment_member (
    environment_id,
    repo,
    pr_number,
    component,
    head_branch,
    head_sha,
    retired_at
)
VALUES ($1, $2, $3, $4, $5, $6, NULL)
ON CONFLICT (environment_id, repo, pr_number)
DO UPDATE SET
    component = EXCLUDED.component,
    head_branch = EXCLUDED.head_branch,
    head_sha = EXCLUDED.head_sha,
    retired_at = NULL
"#,
        )
        .bind(environment_id)
        .bind(&member.key.repo)
        .bind(member.key.number as i64)
        .bind(&member.component)
        .bind(&member.head_branch)
        .bind(&member.head_sha)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn retire_member(&mut self, key: &PullRequestKey) -> anyhow::Result<()> {
        sqlx::query(
            r#"
UPDATE environment_member
SET retired_at = NOW()
WHERE repo = $1
  AND pr_number = $2
  AND retired_at IS NULL
"#,
        )
        .bind(&key.repo)
        .bind(key.number as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn environment_for_pr(
        &self,
        key: &PullRequestKey,
    ) -> anyhow::Result<Option<EnvironmentState>> {
        let row = sqlx::query(
            r#"
SELECT e.id, e.manifest_path, e.is_preview
FROM environment_member AS m
JOIN environment AS e ON e.id = m.environment_id
WHERE m.repo = $1
  AND m.pr_number = $2
  AND m.retired_at IS NULL
  AND e.retired_at IS NULL
ORDER BY m.created_at DESC, m.id DESC
LIMIT 1
"#,
        )
        .bind(&key.repo)
        .bind(key.number as i64)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            Ok(EnvironmentState {
                id: row.try_get("id")?,
                manifest_path: row.try_get("manifest_path")?,
                is_preview: row.try_get("is_preview")?,
            })
        })
        .transpose()
    }

    async fn active_environment(&self, id: &str) -> anyhow::Result<Option<EnvironmentState>> {
        let row = sqlx::query(
            r#"
SELECT id, manifest_path, is_preview
FROM environment
WHERE id = $1
  AND retired_at IS NULL
"#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            Ok(EnvironmentState {
                id: row.try_get("id")?,
                manifest_path: row.try_get("manifest_path")?,
                is_preview: row.try_get("is_preview")?,
            })
        })
        .transpose()
    }

    async fn active_members(
        &self,
        environment_id: &str,
    ) -> anyhow::Result<Vec<PreviewPullRequest>> {
        let rows = sqlx::query(
            r#"
SELECT repo, pr_number, component, head_branch, head_sha
FROM environment_member
WHERE environment_id = $1
  AND retired_at IS NULL
ORDER BY repo, pr_number
"#,
        )
        .bind(environment_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let repo: String = row.try_get("repo")?;
                let number: i64 = row.try_get("pr_number")?;
                let component: Option<String> = row.try_get("component")?;
                let head_branch: Option<String> = row.try_get("head_branch")?;
                let head_sha: Option<String> = row.try_get("head_sha")?;
                Ok(PreviewPullRequest::new(
                    repo,
                    number as u64,
                    component.context("environment member is missing component")?,
                    head_branch.context("environment member is missing head_branch")?,
                    head_sha.context("environment member is missing head_sha")?,
                ))
            })
            .collect()
    }

    async fn record_manifest_revision(
        &mut self,
        environment_id: &str,
        commit_sha: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
INSERT INTO manifest_revision (environment_id, commit_sha)
VALUES ($1, $2)
"#,
        )
        .bind(environment_id)
        .bind(commit_sha)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

fn can_reuse_retired_environment_id(id: &str, is_preview: bool) -> bool {
    is_preview && id.starts_with("preview-") && !is_preview_environment_id(id)
}

pub struct PgQueueStore {
    pool: PgPool,
    lock_conn: Option<PoolConnection<Postgres>>,
}

impl PgQueueStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            lock_conn: None,
        }
    }

    pub async fn pull_requests_for_dev_bump_commit_sha(
        &self,
        commit_sha: &str,
    ) -> anyhow::Result<Vec<PullRequestKey>> {
        let rows = sqlx::query(
            r#"
SELECT DISTINCT cwm.repo, cwm.pr_number
FROM gate_run AS gr
JOIN candidate_world AS cw ON cw.gate_run_id = gr.id
JOIN candidate_world_member AS cwm ON cwm.candidate_world_id = cw.id
WHERE gr.dev_bump_commit_sha = $1
ORDER BY cwm.repo, cwm.pr_number
"#,
        )
        .bind(commit_sha)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let repo: String = row.try_get("repo")?;
                let number: i64 = row.try_get("pr_number")?;
                Ok(PullRequestKey::new(repo, number as u64))
            })
            .collect()
    }
}

impl QueueStore for PgQueueStore {
    async fn try_acquire_global_lock(&mut self) -> anyhow::Result<bool> {
        if self.lock_conn.is_some() {
            return Ok(false);
        }

        let mut conn = self.pool.acquire().await?;
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(GLOBAL_QUEUE_LOCK_KEY)
            .fetch_one(&mut *conn)
            .await?;
        if acquired {
            self.lock_conn = Some(conn);
        }
        Ok(acquired)
    }

    async fn release_global_lock(&mut self) -> anyhow::Result<()> {
        let Some(mut conn) = self.lock_conn.take() else {
            return Ok(());
        };
        let _: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
            .bind(GLOBAL_QUEUE_LOCK_KEY)
            .fetch_one(&mut *conn)
            .await?;
        Ok(())
    }

    async fn oldest_ready_candidate(
        &mut self,
        components: &[RegisteredComponent],
    ) -> anyhow::Result<Option<QueuePullRequest>> {
        if components.is_empty() {
            return Ok(None);
        }

        let repos = components
            .iter()
            .map(|component| component.repo.clone())
            .collect::<Vec<_>>();
        let rows = sqlx::query(
            r#"
SELECT
    pr.repository,
    pr.number,
    pr.head_branch,
    pr.base_branch,
    pr.approved_sha,
    auto_build.commit_sha AS auto_build_commit_sha,
    auto_build.parent AS auto_build_parent,
    auto_build.status AS auto_build_status
FROM pull_request AS pr
LEFT JOIN build AS auto_build ON auto_build.id = pr.auto_build_id
WHERE pr.repository = ANY($1)
  AND pr.status = 'open'
  AND approved_by IS NOT NULL
  AND approved_sha IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM candidate_world_member AS cwm
      JOIN candidate_world AS cw ON cw.id = cwm.candidate_world_id
      JOIN gate_run AS gr ON gr.id = cw.gate_run_id
      WHERE cwm.repo = pr.repository
        AND cwm.pr_number = pr.number
        AND cwm.head_sha = COALESCE(auto_build.commit_sha, pr.approved_sha)
        AND gr.status != 'invalidated'
  )
ORDER BY pr.created_at ASC, pr.id ASC
"#,
        )
        .bind(&repos)
        .fetch_all(&self.pool)
        .await?;

        for row in rows {
            let repo: String = row.try_get("repository")?;
            let component = components
                .iter()
                .find(|component| component.repo == repo)
                .with_context(|| format!("No registered component found for `{repo}`"))?;
            let number: i64 = row.try_get("number")?;
            let head_branch: String = row.try_get("head_branch")?;
            let base_branch: String = row.try_get("base_branch")?;
            let approved_sha: String = row
                .try_get::<Option<String>, _>("approved_sha")?
                .context("ready pull request is missing approved_sha")?;
            let auto_build_commit_sha: Option<String> = row.try_get("auto_build_commit_sha")?;
            let auto_build_parent: Option<String> = row.try_get("auto_build_parent")?;
            let auto_build_status: Option<String> = row.try_get("auto_build_status")?;

            match component.mode {
                ComponentMode::GateOnly if auto_build_commit_sha.is_none() => {
                    return Ok(Some(QueuePullRequest::with_base_branch(
                        repo,
                        number as u64,
                        component.name.clone(),
                        approved_sha.clone(),
                        head_branch,
                        approved_sha,
                        base_branch,
                    )));
                }
                ComponentMode::Chained if auto_build_status.as_deref() == Some("success") => {
                    let tested_commit_sha = auto_build_commit_sha
                        .context("successful auto build is missing commit_sha")?;
                    let tested_base_sha =
                        auto_build_parent.context("successful auto build is missing parent")?;
                    return Ok(Some(
                        QueuePullRequest::with_base_branch(
                            repo,
                            number as u64,
                            component.name.clone(),
                            approved_sha.clone(),
                            head_branch,
                            approved_sha,
                            base_branch,
                        )
                        .with_repo_validation(tested_commit_sha, tested_base_sha),
                    ));
                }
                _ => {}
            }
        }

        Ok(None)
    }

    async fn oldest_resumable_merge(&mut self) -> anyhow::Result<Option<RecordedGateRun>> {
        let resumable_statuses = [GATE_PASSED_STATUS, MERGING_PR_STATUS, BUMPING_DEV_STATUS];
        let row = sqlx::query(
            r#"
SELECT id, external_id, status, world::text AS world, merge_commit_sha, dev_bump_commit_sha
FROM gate_run
WHERE status = ANY($1)
  AND world IS NOT NULL
ORDER BY updated_at ASC, id ASC
LIMIT 1
"#,
        )
        .bind(resumable_statuses.as_slice())
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            let world: String = row.try_get("world")?;
            Ok(RecordedGateRun {
                id: row.try_get("id")?,
                external_id: row.try_get("external_id")?,
                status: row.try_get("status")?,
                world: serde_json::from_str(&world).context("Cannot parse stored gate world")?,
                merge_commit_sha: row.try_get("merge_commit_sha")?,
                dev_bump_commit_sha: row.try_get("dev_bump_commit_sha")?,
            })
        })
        .transpose()
    }

    async fn oldest_resumable_gate(&mut self) -> anyhow::Result<Option<RecordedGateRun>> {
        let row = sqlx::query(
            r#"
SELECT id, external_id, status, world::text AS world, merge_commit_sha, dev_bump_commit_sha
FROM gate_run
WHERE status = $1
  AND world IS NOT NULL
ORDER BY updated_at ASC, id ASC
LIMIT 1
"#,
        )
        .bind(RUNNING_STATUS)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            let world: String = row.try_get("world")?;
            Ok(RecordedGateRun {
                id: row.try_get("id")?,
                external_id: row.try_get("external_id")?,
                status: row.try_get("status")?,
                world: serde_json::from_str(&world).context("Cannot parse stored gate world")?,
                merge_commit_sha: row.try_get("merge_commit_sha")?,
                dev_bump_commit_sha: row.try_get("dev_bump_commit_sha")?,
            })
        })
        .transpose()
    }

    async fn record_gate_run(&mut self, world: &CandidateWorld) -> anyhow::Result<RecordedGateRun> {
        let external_id = gate_external_id(world)?;
        let world_json = serde_json::to_string(world).context("Cannot serialize gate world")?;
        let mut tx = self.pool.begin().await?;
        // Remove any previously-invalidated run with the same external_id so the
        // INSERT below cannot conflict on the unique constraint.
        sqlx::query(r#"DELETE FROM gate_run WHERE external_id = $1 AND status = 'invalidated'"#)
            .bind(&external_id)
            .execute(&mut *tx)
            .await?;
        let gate_run_id: i64 = sqlx::query_scalar(
            r#"
INSERT INTO gate_run (external_id, status, world)
VALUES ($1, $2, $3::jsonb)
RETURNING id
"#,
        )
        .bind(&external_id)
        .bind(PENDING_STATUS)
        .bind(&world_json)
        .fetch_one(&mut *tx)
        .await?;

        let candidate_world_id: i64 = sqlx::query_scalar(
            r#"
INSERT INTO candidate_world (gate_run_id)
VALUES ($1)
RETURNING id
"#,
        )
        .bind(gate_run_id)
        .fetch_one(&mut *tx)
        .await?;

        for member in &world.members {
            sqlx::query(
                r#"
INSERT INTO candidate_world_member (
    candidate_world_id,
    repo,
    pr_number,
    head_sha
)
VALUES ($1, $2, $3, $4)
"#,
            )
            .bind(candidate_world_id)
            .bind(&member.key.repo)
            .bind(member.key.number as i64)
            .bind(&member.head_sha)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(RecordedGateRun {
            id: gate_run_id,
            external_id,
            status: PENDING_STATUS.to_string(),
            world: world.clone(),
            merge_commit_sha: None,
            dev_bump_commit_sha: None,
        })
    }

    async fn mark_gate_run_status(
        &mut self,
        external_id: &str,
        status: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
UPDATE gate_run
SET status = $2, updated_at = NOW()
WHERE external_id = $1
"#,
        )
        .bind(external_id)
        .bind(status)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_gate_run_diagnostic(
        &mut self,
        external_id: &str,
        diagnostic: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
UPDATE gate_run
SET diagnostic = $2, updated_at = NOW()
WHERE external_id = $1
"#,
        )
        .bind(external_id)
        .bind(diagnostic)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn latest_gate_status(&self, key: &PullRequestKey) -> anyhow::Result<Option<GateStatus>> {
        let row = sqlx::query(
            r#"
SELECT gr.external_id, cwm.head_sha, gr.status, gr.diagnostic
FROM gate_run AS gr
JOIN candidate_world AS cw ON cw.gate_run_id = gr.id
JOIN candidate_world_member AS cwm ON cwm.candidate_world_id = cw.id
WHERE cwm.repo = $1
  AND cwm.pr_number = $2
ORDER BY gr.created_at DESC, gr.id DESC
LIMIT 1
"#,
        )
        .bind(&key.repo)
        .bind(key.number as i64)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            Ok(GateStatus {
                external_id: row.try_get("external_id")?,
                head_sha: row.try_get("head_sha")?,
                status: row.try_get("status")?,
                diagnostic: row.try_get("diagnostic")?,
            })
        })
        .transpose()
    }

    async fn invalidate_active_gate_runs(
        &mut self,
        key: &PullRequestKey,
    ) -> anyhow::Result<Vec<GateStatus>> {
        let active_statuses = [PENDING_STATUS, PENDING_EXECUTOR_STATUS, RUNNING_STATUS];
        let rows = sqlx::query(
            r#"
UPDATE gate_run AS gr
SET status = $3, updated_at = NOW()
FROM candidate_world AS cw
JOIN candidate_world_member AS cwm ON cwm.candidate_world_id = cw.id
WHERE cw.gate_run_id = gr.id
  AND cwm.repo = $1
  AND cwm.pr_number = $2
  AND gr.status = ANY($4)
RETURNING gr.external_id, cwm.head_sha, gr.status, gr.diagnostic
"#,
        )
        .bind(&key.repo)
        .bind(key.number as i64)
        .bind(INVALIDATED_STATUS)
        .bind(active_statuses.as_slice())
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(GateStatus {
                    external_id: row.try_get("external_id")?,
                    head_sha: row.try_get("head_sha")?,
                    status: row.try_get("status")?,
                    diagnostic: row.try_get("diagnostic")?,
                })
            })
            .collect()
    }

    async fn reenqueue_pr(&mut self, pr: &QueuePullRequest) -> anyhow::Result<()> {
        sqlx::query(
            r#"
UPDATE pull_request
SET approved_sha = $3,
    head_branch = $4,
    auto_build_id = NULL
WHERE repository = $1
  AND number = $2
  AND approved_by IS NOT NULL
"#,
        )
        .bind(&pr.key.repo)
        .bind(pr.key.number as i64)
        .bind(&pr.head_sha)
        .bind(&pr.head_branch)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn clear_repo_validation(&mut self, key: &PullRequestKey) -> anyhow::Result<()> {
        sqlx::query(
            r#"
UPDATE pull_request
SET auto_build_id = NULL
WHERE repository = $1
  AND number = $2
  AND approved_by IS NOT NULL
"#,
        )
        .bind(&key.repo)
        .bind(key.number as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

impl AdvisoryGateStore for PgQueueStore {
    async fn record_advisory_gate_status(
        &mut self,
        pr: &QueuePullRequest,
        external_id: &str,
        status: &str,
        diagnostic: Option<&str>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
INSERT INTO advisory_gate_run (repo, pr_number, head_sha, external_id, status, diagnostic)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (external_id)
DO UPDATE SET status = EXCLUDED.status, diagnostic = EXCLUDED.diagnostic, updated_at = NOW()
"#,
        )
        .bind(&pr.key.repo)
        .bind(pr.key.number as i64)
        .bind(&pr.head_sha)
        .bind(external_id)
        .bind(status)
        .bind(diagnostic)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn latest_advisory_gate_status(
        &self,
        key: &PullRequestKey,
    ) -> anyhow::Result<Option<GateStatus>> {
        let row = sqlx::query(
            r#"
SELECT external_id, head_sha, status, diagnostic
FROM advisory_gate_run
WHERE repo = $1
  AND pr_number = $2
ORDER BY created_at DESC, id DESC
LIMIT 1
"#,
        )
        .bind(&key.repo)
        .bind(key.number as i64)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            Ok(GateStatus {
                external_id: row.try_get("external_id")?,
                head_sha: row.try_get("head_sha")?,
                status: row.try_get("status")?,
                diagnostic: row.try_get("diagnostic")?,
            })
        })
        .transpose()
    }
}

impl MergeStore for PgQueueStore {
    async fn mark_gate_run_status(&self, external_id: &str, status: &str) -> anyhow::Result<()> {
        sqlx::query(
            r#"
UPDATE gate_run
SET status = $2, updated_at = NOW()
WHERE external_id = $1
"#,
        )
        .bind(external_id)
        .bind(status)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_merge_commit_sha(
        &self,
        external_id: &str,
        merge_commit_sha: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
UPDATE gate_run
SET merge_commit_sha = $2, updated_at = NOW()
WHERE external_id = $1
"#,
        )
        .bind(external_id)
        .bind(merge_commit_sha)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_dev_bump_commit_sha(
        &self,
        external_id: &str,
        dev_bump_commit_sha: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
UPDATE gate_run
SET dev_bump_commit_sha = $2, updated_at = NOW()
WHERE external_id = $1
"#,
        )
        .bind(external_id)
        .bind(dev_bump_commit_sha)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
