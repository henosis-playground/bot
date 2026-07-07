use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

/// The lockfile schema shared by the Henosis renderer and bot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lockfile {
    pub environment: EnvironmentSection,
    pub components: IndexMap<String, ComponentEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentSection {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum ComponentEntry {
    Pinned(PinnedEntry),
    Follower(FollowerEntry),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedEntry {
    pub repo: String,
    pub r#ref: String,
    pub digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowerEntry {
    pub follow: String,
}

pub fn parse_toml(content: &str) -> Result<Lockfile, toml::de::Error> {
    toml::from_str(content)
}

pub fn to_toml(lockfile: &Lockfile) -> Result<String, toml::ser::Error> {
    toml::to_string(lockfile)
}

pub fn pinned(
    repo: impl Into<String>,
    r#ref: impl Into<String>,
    digest: impl Into<String>,
) -> ComponentEntry {
    ComponentEntry::Pinned(PinnedEntry {
        repo: repo.into(),
        r#ref: r#ref.into(),
        digest: digest.into(),
    })
}

pub fn follower_dev() -> ComponentEntry {
    ComponentEntry::Follower(FollowerEntry {
        follow: "dev".to_string(),
    })
}

pub fn validate(lockfile: &Lockfile) -> anyhow::Result<()> {
    let stable = matches!(lockfile.environment.id.as_str(), "dev" | "staging" | "prod");
    if stable {
        for (name, entry) in &lockfile.components {
            if matches!(entry, ComponentEntry::Follower(_)) {
                anyhow::bail!(
                    "follower entry for component `{name}` is invalid in {}",
                    lockfile.environment.id
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA_EXAMPLE: &str = r#"
# deploy repo: dev.toml | staging.toml | prod.toml | preview lockfiles (e.g. pr-service-a-3.toml)

[environment]
id = "dev"   # canonical EnvId; must match the filename

# --- Pinned entry: resolved AND executed (rendered) in this environment ---
[components.service-a]
repo = "henosis-playground/service-a"
ref = "0a1b2c3d..."      # git commit sha. dev/staging/prod MUST pin shas.
                          # Preview lockfiles may use a branch name here (the PR branch).
digest = "sha256:..."    # image digest; ref and digest travel as one unit (poc.md fixed decision)

# --- Follower entry (preview lockfiles only): track dev ---
[components.service-b]
follow = "dev"
"#;

    const PINNED_ONLY_EXAMPLE: &str = r#"
[environment]
id = "dev"

[components.service-a]
repo = "henosis-playground/service-a"
ref = "0a1b2c3d"
digest = "sha256:aaaa"

[components.service-b]
repo = "henosis-playground/service-b"
ref = "1b2c3d4e"
digest = "sha256:bbbb"
"#;

    #[test]
    fn parses_schema_example() {
        let lockfile: Lockfile = toml::from_str(SCHEMA_EXAMPLE).unwrap();

        assert_eq!(lockfile.environment.id, "dev");
        assert!(matches!(
            lockfile.components.get("service-a"),
            Some(ComponentEntry::Pinned(PinnedEntry { repo, r#ref, digest }))
                if repo == "henosis-playground/service-a"
                    && r#ref == "0a1b2c3d..."
                    && digest == "sha256:..."
        ));
        assert!(matches!(
            lockfile.components.get("service-b"),
            Some(ComponentEntry::Follower(FollowerEntry { follow })) if follow == "dev"
        ));
    }

    #[test]
    fn round_trips_schema_example() {
        let lockfile: Lockfile = parse_toml(SCHEMA_EXAMPLE).unwrap();
        let serialized = to_toml(&lockfile).unwrap();
        let reparsed: Lockfile = parse_toml(&serialized).unwrap();

        assert_eq!(lockfile, reparsed);
    }

    #[test]
    fn round_trips_pinned_entries() {
        let lockfile: Lockfile = parse_toml(PINNED_ONLY_EXAMPLE).unwrap();
        let serialized = to_toml(&lockfile).unwrap();
        let reparsed: Lockfile = parse_toml(&serialized).unwrap();

        assert_eq!(lockfile, reparsed);
    }

    #[test]
    fn rejects_unknown_component_field() {
        let content = r#"
[environment]
id = "dev"

[components.service-a]
repo = "henosis-playground/service-a"
ref = "0a1b2c3d"
digest = "sha256:aaaa"
unexpected = true
"#;

        assert!(toml::from_str::<Lockfile>(content).is_err());
    }

    #[test]
    fn rejects_followers_in_stable_lockfiles() {
        let lockfile = Lockfile {
            environment: EnvironmentSection {
                id: "dev".to_string(),
            },
            components: IndexMap::from([("service-a".to_string(), follower_dev())]),
        };

        assert!(validate(&lockfile).is_err());
    }
}
