use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::Context;
use serde::Deserialize;

pub const HENOSIS_CONFIG_ENV: &str = "HENOSIS_CONFIG";

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HenosisConfig {
    pub deploy_repo: String,
    #[serde(default = "default_lockfile_branch")]
    pub lockfile_branch: String,
    #[serde(default = "default_gate_command")]
    pub gate_command: String,
    #[serde(default = "default_gate_check_run_name")]
    pub gate_check_run_name: String,
    #[serde(default = "default_queue_tick_interval_secs")]
    pub queue_tick_interval_secs: u64,
    #[serde(default = "default_cmd_prefix")]
    pub cmd_prefix: String,
    #[serde(default)]
    pub components: Vec<ComponentConfig>,
    #[serde(default)]
    pub source_repos: Vec<String>,
    #[serde(default = "default_dev_lockfile_path")]
    pub dev_lockfile_path: String,
    pub environments: Vec<EnvironmentConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentConfig {
    pub name: String,
    pub repo: String,
    #[serde(default = "default_main_branch")]
    pub main_branch: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredComponent {
    pub name: String,
    pub repo: String,
    pub main_branch: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentConfig {
    pub id: String,
    pub lockfile_path: String,
}

impl HenosisConfig {
    pub fn load_from_env() -> anyhow::Result<Self> {
        let path = std::env::var(HENOSIS_CONFIG_ENV)
            .with_context(|| format!("{HENOSIS_CONFIG_ENV} is not set"))?;
        Self::load_from_path(path)
    }

    pub fn load_from_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let content = fs::read_to_string(path)
            .with_context(|| format!("Cannot read Henosis config from {}", path.display()))?;
        toml::from_str(&content)
            .with_context(|| format!("Cannot parse Henosis config from {}", path.display()))
    }

    pub fn registered_components(&self) -> Vec<RegisteredComponent> {
        if !self.components.is_empty() {
            return self
                .components
                .iter()
                .map(|component| RegisteredComponent {
                    name: component.name.clone(),
                    repo: component.repo.clone(),
                    main_branch: component.main_branch.clone(),
                })
                .collect();
        }

        self.source_repos
            .iter()
            .map(|repo| RegisteredComponent {
                name: component_name_from_repo(repo),
                repo: repo.clone(),
                main_branch: default_main_branch(),
            })
            .collect()
    }

    pub fn is_component_repo(&self, repo: &str) -> bool {
        self.registered_components()
            .iter()
            .any(|component| component.repo == repo)
    }

    pub fn component_for_repo(&self, repo: &str) -> Option<RegisteredComponent> {
        self.registered_components()
            .into_iter()
            .find(|component| component.repo == repo)
    }

    pub fn queue_tick_interval(&self) -> Duration {
        Duration::from_secs(self.queue_tick_interval_secs.max(1))
    }

    pub fn environment_lockfile_path(&self, environment_id: &str) -> String {
        self.environments
            .iter()
            .find(|env| env.id == environment_id)
            .map(|env| env.lockfile_path.clone())
            .unwrap_or_else(|| format!("{environment_id}.toml"))
    }
}

fn default_lockfile_branch() -> String {
    "main".to_string()
}

fn default_gate_command() -> String {
    "henosis-gate".to_string()
}

fn default_gate_check_run_name() -> String {
    "Henosis gate".to_string()
}

fn default_queue_tick_interval_secs() -> u64 {
    15
}

fn default_cmd_prefix() -> String {
    "@henosis-bot".to_string()
}

fn default_dev_lockfile_path() -> String {
    "dev.toml".to_string()
}

fn default_main_branch() -> String {
    "main".to_string()
}

fn component_name_from_repo(repo: &str) -> String {
    repo.rsplit('/').next().unwrap_or(repo).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_config(content: &str) -> Result<HenosisConfig, toml::de::Error> {
        toml::from_str(content)
    }

    #[test]
    fn parses_config_with_defaults() {
        let config = parse_config(
            r#"
deploy_repo = "henosis-playground/deploy"
source_repos = ["henosis-playground/service-a", "henosis-playground/service-b"]

[[environments]]
id = "dev"
lockfile_path = "dev.toml"
"#,
        )
        .unwrap();

        assert_eq!(config.lockfile_branch, "main");
        assert_eq!(config.gate_command, "henosis-gate");
        assert_eq!(config.gate_check_run_name, "Henosis gate");
        assert_eq!(config.queue_tick_interval_secs, 15);
        assert_eq!(config.queue_tick_interval(), Duration::from_secs(15));
        assert_eq!(config.cmd_prefix, "@henosis-bot");
        assert_eq!(config.environments[0].id, "dev");
        assert_eq!(
            config.registered_components(),
            vec![
                RegisteredComponent {
                    name: "service-a".to_string(),
                    repo: "henosis-playground/service-a".to_string(),
                    main_branch: "main".to_string(),
                },
                RegisteredComponent {
                    name: "service-b".to_string(),
                    repo: "henosis-playground/service-b".to_string(),
                    main_branch: "main".to_string(),
                }
            ]
        );
    }

    #[test]
    fn parses_config_with_overrides() {
        let config = parse_config(
            r#"
deploy_repo = "henosis-playground/deploy"
lockfile_branch = "lockfiles"
gate_command = "custom-gate"
gate_check_run_name = "Custom gate"
queue_tick_interval_secs = 3
cmd_prefix = "@custom-bot"
source_repos = ["henosis-playground/service-a"]

[[environments]]
id = "staging"
lockfile_path = "staging.toml"
"#,
        )
        .unwrap();

        assert_eq!(config.lockfile_branch, "lockfiles");
        assert_eq!(config.gate_command, "custom-gate");
        assert_eq!(config.gate_check_run_name, "Custom gate");
        assert_eq!(config.queue_tick_interval(), Duration::from_secs(3));
        assert_eq!(config.cmd_prefix, "@custom-bot");
    }

    #[test]
    fn parses_component_registry() {
        let config = parse_config(
            r#"
deploy_repo = "henosis-playground/deploy"

[[components]]
name = "api"
repo = "henosis-playground/service-a"
main_branch = "trunk"

[[environments]]
id = "dev"
lockfile_path = "dev.toml"
"#,
        )
        .unwrap();

        assert_eq!(
            config.registered_components(),
            vec![RegisteredComponent {
                name: "api".to_string(),
                repo: "henosis-playground/service-a".to_string(),
                main_branch: "trunk".to_string(),
            }]
        );
        assert!(config.is_component_repo("henosis-playground/service-a"));
        assert!(!config.is_component_repo("henosis-playground/service-b"));
    }

    #[test]
    fn rejects_unknown_top_level_field() {
        let result = parse_config(
            r#"
deploy_repo = "henosis-playground/deploy"
source_repos = []
unexpected = true
environments = []
"#,
        );

        assert!(result.is_err());
    }

    #[test]
    fn rejects_unknown_environment_field() {
        let result = parse_config(
            r#"
deploy_repo = "henosis-playground/deploy"
source_repos = []

[[environments]]
id = "dev"
lockfile_path = "dev.toml"
unexpected = true
"#,
        );

        assert!(result.is_err());
    }
}
