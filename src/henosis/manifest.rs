use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// The manifest schema shared by the Henosis renderer and bot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
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

pub fn parse_toml(content: &str) -> Result<Manifest, toml::de::Error> {
    toml::from_str(content)
}

pub fn to_toml(manifest: &Manifest) -> Result<String, toml::ser::Error> {
    toml::to_string(manifest)
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

pub fn synthetic_digest_for_ref(r#ref: &str) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(r#ref.as_bytes())))
}

pub fn validate(manifest: &Manifest) -> anyhow::Result<()> {
    let stable = matches!(manifest.environment.id.as_str(), "dev" | "staging" | "prod");
    if stable {
        for (name, entry) in &manifest.components {
            if matches!(entry, ComponentEntry::Follower(_)) {
                anyhow::bail!(
                    "follower entry for component `{name}` is invalid in {}",
                    manifest.environment.id
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
# deploy repo: dev.toml | staging.toml | prod.toml | preview manifests (e.g. pr-service-a-3.toml)

[environment]
id = "dev"   # canonical EnvId; must match the filename

# --- Pinned entry: resolved AND executed (rendered) in this environment ---
[components.service-a]
repo = "henosis-playground/service-a"
ref = "0a1b2c3d..."      # git commit sha. dev/staging/prod MUST pin shas.
                          # Preview manifests may use a branch name here (the PR branch).
digest = "sha256:..."    # image digest; ref and digest travel as one unit (poc.md fixed decision)

# --- Follower entry (preview manifests only): track dev ---
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
        let manifest: Manifest = toml::from_str(SCHEMA_EXAMPLE).unwrap();

        assert_eq!(manifest.environment.id, "dev");
        assert!(matches!(
            manifest.components.get("service-a"),
            Some(ComponentEntry::Pinned(PinnedEntry { repo, r#ref, digest }))
                if repo == "henosis-playground/service-a"
                    && r#ref == "0a1b2c3d..."
                    && digest == "sha256:..."
        ));
        assert!(matches!(
            manifest.components.get("service-b"),
            Some(ComponentEntry::Follower(FollowerEntry { follow })) if follow == "dev"
        ));
    }

    #[test]
    fn round_trips_schema_example() {
        let manifest: Manifest = parse_toml(SCHEMA_EXAMPLE).unwrap();
        let serialized = to_toml(&manifest).unwrap();
        let reparsed: Manifest = parse_toml(&serialized).unwrap();

        assert_eq!(manifest, reparsed);
    }

    #[test]
    fn round_trips_pinned_entries() {
        let manifest: Manifest = parse_toml(PINNED_ONLY_EXAMPLE).unwrap();
        let serialized = to_toml(&manifest).unwrap();
        let reparsed: Manifest = parse_toml(&serialized).unwrap();

        assert_eq!(manifest, reparsed);
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

        assert!(toml::from_str::<Manifest>(content).is_err());
    }

    #[test]
    fn rejects_followers_in_stable_manifests() {
        let manifest = Manifest {
            environment: EnvironmentSection {
                id: "dev".to_string(),
            },
            components: IndexMap::from([("service-a".to_string(), follower_dev())]),
        };

        assert!(validate(&manifest).is_err());
    }

    #[test]
    fn synthetic_digest_moves_with_ref() {
        let a = synthetic_digest_for_ref("a-ref");
        let b = synthetic_digest_for_ref("b-ref");

        assert!(a.starts_with("sha256:"));
        assert_eq!(a.len(), "sha256:".len() + 64);
        assert_ne!(a, b);
    }
}
