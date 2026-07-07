use std::fs;
use std::path::Path;

use anyhow::Context;
use serde::Deserialize;

pub const HENOSIS_CONFIG_ENV: &str = "HENOSIS_CONFIG";

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HenosisConfig {
    pub deploy_repo: String,
    #[serde(default = "default_lockfile_branch")]
    pub lockfile_branch: String,
    #[serde(default = "default_gate_check_run_name")]
    pub gate_check_run_name: String,
    #[serde(default = "default_cmd_prefix")]
    pub cmd_prefix: String,
    pub source_repos: Vec<String>,
    pub environments: Vec<EnvironmentConfig>,
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
}

fn default_lockfile_branch() -> String {
    "main".to_string()
}

fn default_gate_check_run_name() -> String {
    "Henosis gate".to_string()
}

fn default_cmd_prefix() -> String {
    "@henosis-bot".to_string()
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
        assert_eq!(config.gate_check_run_name, "Henosis gate");
        assert_eq!(config.cmd_prefix, "@henosis-bot");
        assert_eq!(config.environments[0].id, "dev");
    }

    #[test]
    fn parses_config_with_overrides() {
        let config = parse_config(
            r#"
deploy_repo = "henosis-playground/deploy"
lockfile_branch = "lockfiles"
gate_check_run_name = "Custom gate"
cmd_prefix = "@custom-bot"
source_repos = ["henosis-playground/service-a"]

[[environments]]
id = "staging"
lockfile_path = "staging.toml"
"#,
        )
        .unwrap();

        assert_eq!(config.lockfile_branch, "lockfiles");
        assert_eq!(config.gate_check_run_name, "Custom gate");
        assert_eq!(config.cmd_prefix, "@custom-bot");
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
