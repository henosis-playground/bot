use std::collections::{BTreeMap, BTreeSet, VecDeque};

use anyhow::Context;
use indexmap::IndexMap;
use serde::Deserialize;

use crate::henosis::config::RegisteredComponent;

const HENOSIS_SCOPE: &str = "@henosis/";
const HENOSIS_SDK_PACKAGE: &str = "@henosis/sdk";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentRef {
    pub name: String,
    pub repo: String,
    pub sha: String,
}

impl ComponentRef {
    pub fn new(name: impl Into<String>, repo: impl Into<String>, sha: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            repo: repo.into(),
            sha: sha.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PackageJson {
    pub name: String,
    #[serde(default)]
    pub dependencies: BTreeMap<String, String>,
    #[serde(default)]
    pub henosis: PackageHenosis,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct PackageHenosis {
    pub component: Option<String>,
    #[serde(default)]
    pub surface: bool,
}

pub trait ComponentPackageReader {
    async fn fetch_package_json(&self, repo: &str, sha: &str) -> anyhow::Result<PackageJson>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentNode {
    pub name: String,
    pub package_name: String,
    pub repo: String,
    pub sha: String,
    pub surface: bool,
    pub dependencies: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentGraph {
    nodes: IndexMap<String, ComponentNode>,
    reverse_edges: BTreeMap<String, BTreeSet<String>>,
}

impl ComponentGraph {
    pub async fn read<R: ComponentPackageReader>(
        components: &[ComponentRef],
        reader: &R,
    ) -> anyhow::Result<Self> {
        let mut packages = Vec::with_capacity(components.len());
        for component in components {
            let package = reader
                .fetch_package_json(&component.repo, &component.sha)
                .await
                .with_context(|| {
                    format!(
                        "Cannot fetch henosis/package.json for {} at {}",
                        component.repo, component.sha
                    )
                })?;
            packages.push((component, package));
        }

        let package_to_component = packages
            .iter()
            .map(|(registered, package)| {
                (
                    package.name.clone(),
                    package
                        .henosis
                        .component
                        .clone()
                        .unwrap_or_else(|| registered.name.clone()),
                )
            })
            .collect::<BTreeMap<_, _>>();

        let mut nodes = IndexMap::new();
        let mut reverse_edges: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (registered, package) in packages {
            let name = package
                .henosis
                .component
                .clone()
                .unwrap_or_else(|| registered.name.clone());
            let dependencies = package
                .dependencies
                .keys()
                .filter(|package_name| is_henosis_component_dependency(package_name))
                .filter_map(|package_name| {
                    component_name_for_package(package_name, &package_to_component)
                })
                .collect::<BTreeSet<_>>();

            for dependency in &dependencies {
                reverse_edges
                    .entry(dependency.clone())
                    .or_default()
                    .insert(name.clone());
            }

            nodes.insert(
                name.clone(),
                ComponentNode {
                    name,
                    package_name: package.name,
                    repo: registered.repo.clone(),
                    sha: registered.sha.clone(),
                    surface: package.henosis.surface,
                    dependencies,
                },
            );
        }

        Ok(Self {
            nodes,
            reverse_edges,
        })
    }

    pub fn from_registered_components(
        components: &[RegisteredComponent],
        refs: &BTreeMap<String, String>,
    ) -> anyhow::Result<Vec<ComponentRef>> {
        components
            .iter()
            .map(|component| {
                let sha = refs.get(&component.name).with_context(|| {
                    format!("No pinned ref found for component `{}`", component.name)
                })?;
                Ok(ComponentRef::new(
                    component.name.clone(),
                    component.repo.clone(),
                    sha.clone(),
                ))
            })
            .collect()
    }

    pub fn nodes(&self) -> &IndexMap<String, ComponentNode> {
        &self.nodes
    }

    pub fn node(&self, name: &str) -> Option<&ComponentNode> {
        self.nodes.get(name)
    }

    pub fn preview_closure<I, S>(&self, changed_components: I) -> BTreeSet<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut closure = BTreeSet::new();
        let mut queue = VecDeque::new();

        for component in changed_components {
            let component = component.as_ref().to_string();
            if self.nodes.contains_key(&component) && closure.insert(component.clone()) {
                queue.push_back(component);
            }
        }

        while let Some(component) = queue.pop_front() {
            if self
                .nodes
                .get(&component)
                .map(|node| node.surface)
                .unwrap_or(false)
            {
                continue;
            }

            let Some(dependents) = self.reverse_edges.get(&component) else {
                continue;
            };

            for dependent in dependents {
                if closure.insert(dependent.clone()) {
                    queue.push_back(dependent.clone());
                }
            }
        }

        closure
    }
}

fn is_henosis_component_dependency(package_name: &str) -> bool {
    package_name.starts_with(HENOSIS_SCOPE) && package_name != HENOSIS_SDK_PACKAGE
}

fn component_name_for_package(
    package_name: &str,
    package_to_component: &BTreeMap<String, String>,
) -> Option<String> {
    package_to_component
        .get(package_name)
        .cloned()
        .or_else(|| package_name.strip_prefix(HENOSIS_SCOPE).map(str::to_string))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct InMemoryPackageReader {
        packages: BTreeMap<(String, String), PackageJson>,
    }

    impl InMemoryPackageReader {
        fn new(packages: Vec<(&str, &str, PackageJson)>) -> Self {
            Self {
                packages: packages
                    .into_iter()
                    .map(|(repo, sha, package)| ((repo.to_string(), sha.to_string()), package))
                    .collect(),
            }
        }
    }

    impl ComponentPackageReader for InMemoryPackageReader {
        async fn fetch_package_json(&self, repo: &str, sha: &str) -> anyhow::Result<PackageJson> {
            self.packages
                .get(&(repo.to_string(), sha.to_string()))
                .cloned()
                .with_context(|| format!("missing package for {repo}@{sha}"))
        }
    }

    fn package(name: &str, component: &str, surface: bool, deps: &[&str]) -> PackageJson {
        PackageJson {
            name: name.to_string(),
            dependencies: deps
                .iter()
                .map(|dep| (dep.to_string(), "workspace:*".to_string()))
                .collect(),
            henosis: PackageHenosis {
                component: Some(component.to_string()),
                surface,
            },
        }
    }

    #[tokio::test]
    async fn closure_walks_reverse_edges_to_nearest_surface() {
        let reader = InMemoryPackageReader::new(vec![
            (
                "henosis-playground/service-a",
                "a-main",
                package("@henosis/service-a", "service-a", false, &["@henosis/sdk"]),
            ),
            (
                "henosis-playground/service-b",
                "b-main",
                package(
                    "@henosis/service-b",
                    "service-b",
                    true,
                    &["@henosis/sdk", "@henosis/service-a"],
                ),
            ),
        ]);
        let components = vec![
            ComponentRef::new("service-a", "henosis-playground/service-a", "a-main"),
            ComponentRef::new("service-b", "henosis-playground/service-b", "b-main"),
        ];

        let graph = ComponentGraph::read(&components, &reader).await.unwrap();

        assert_eq!(
            graph.preview_closure(["service-a"]),
            BTreeSet::from(["service-a".to_string(), "service-b".to_string()])
        );
        assert_eq!(
            graph.preview_closure(["service-b"]),
            BTreeSet::from(["service-b".to_string()])
        );
    }
}
