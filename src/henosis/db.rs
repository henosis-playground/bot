use anyhow::Context;
use sqlx::pool::PoolConnection;
use sqlx::{PgPool, Postgres, Row};

use crate::henosis::config::RegisteredComponent;
use crate::henosis::environment::{
    EnvironmentState, EnvironmentStore, PreviewPullRequest, PullRequestKey,
};
use crate::henosis::queue::{
    CandidateWorld, GLOBAL_QUEUE_LOCK_KEY, GateStatus, QueuePullRequest, QueueStore,
    RecordedGateRun, gate_external_id,
};

pub struct PgEnvironmentStore {
    pool: PgPool,
}

impl PgEnvironmentStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl EnvironmentStore for PgEnvironmentStore {
    async fn upsert_environment(
        &mut self,
        id: &str,
        lockfile_path: &str,
        is_preview: bool,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
INSERT INTO environment (id, lockfile_path, is_preview, retired_at, updated_at)
VALUES ($1, $2, $3, NULL, NOW())
ON CONFLICT (id)
DO UPDATE SET
    lockfile_path = EXCLUDED.lockfile_path,
    is_preview = EXCLUDED.is_preview,
    retired_at = NULL,
    updated_at = NOW()
"#,
        )
        .bind(id)
        .bind(lockfile_path)
        .bind(is_preview)
        .execute(&self.pool)
        .await?;
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
SELECT e.id, e.lockfile_path, e.is_preview
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
                lockfile_path: row.try_get("lockfile_path")?,
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

    async fn record_lockfile_revision(
        &mut self,
        environment_id: &str,
        commit_sha: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
INSERT INTO lockfile_revision (environment_id, commit_sha)
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
        let row = sqlx::query(
            r#"
SELECT repository, number, head_branch, approved_sha
FROM pull_request AS pr
WHERE repository = ANY($1)
  AND status = 'open'
  AND approved_by IS NOT NULL
  AND approved_sha IS NOT NULL
  AND auto_build_id IS NULL
  AND NOT EXISTS (
      SELECT 1
      FROM candidate_world_member AS cwm
      JOIN candidate_world AS cw ON cw.id = cwm.candidate_world_id
      JOIN gate_run AS gr ON gr.id = cw.gate_run_id
      WHERE cwm.repo = pr.repository
        AND cwm.pr_number = pr.number
        AND cwm.head_sha = pr.approved_sha
        AND gr.status IN ('pending', 'pending-executor', 'running')
  )
ORDER BY pr.created_at ASC, pr.id ASC
LIMIT 1
"#,
        )
        .bind(&repos)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        let repo: String = row.try_get("repository")?;
        let component = components
            .iter()
            .find(|component| component.repo == repo)
            .with_context(|| format!("No registered component found for `{repo}`"))?;
        let number: i32 = row.try_get("number")?;
        let head_branch: String = row.try_get("head_branch")?;
        let approved_sha: String = row
            .try_get::<Option<String>, _>("approved_sha")?
            .context("ready pull request is missing approved_sha")?;

        Ok(Some(QueuePullRequest::new(
            repo,
            number as u64,
            component.name.clone(),
            approved_sha.clone(),
            head_branch,
            approved_sha,
        )))
    }

    async fn record_gate_run(&mut self, world: &CandidateWorld) -> anyhow::Result<RecordedGateRun> {
        let external_id = gate_external_id(world)?;
        let mut tx = self.pool.begin().await?;
        let gate_run_id: i64 = sqlx::query_scalar(
            r#"
INSERT INTO gate_run (external_id, status)
VALUES ($1, 'pending')
RETURNING id
"#,
        )
        .bind(&external_id)
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
            world: world.clone(),
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

    async fn latest_gate_status(&self, key: &PullRequestKey) -> anyhow::Result<Option<GateStatus>> {
        let row = sqlx::query(
            r#"
SELECT gr.external_id, gr.status
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
                status: row.try_get("status")?,
            })
        })
        .transpose()
    }
}
