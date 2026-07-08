use std::collections::{BTreeMap, BTreeSet, VecDeque};

use anyhow::{Context, anyhow};
use indexmap::IndexMap;
use serde::Deserialize;

use crate::henosis::config::RegisteredComponent;

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
            .map(|(component, package)| {
                validate_component_identity(component, package)?;
                Ok((package.name.clone(), component.name.clone()))
            })
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;

        let mut nodes = IndexMap::new();
        let mut reverse_edges: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (component, package) in packages {
            let name = component.name.clone();
            let dependencies = package
                .dependencies
                .keys()
                .filter_map(|package_name| package_to_component.get(package_name).cloned())
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
                    repo: component.repo.clone(),
                    sha: component.sha.clone(),
                    dependencies,
                },
            );
        }

        let graph = Self {
            nodes,
            reverse_edges,
        };
        graph.ensure_acyclic()?;
        Ok(graph)
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

    fn ensure_acyclic(&self) -> anyhow::Result<()> {
        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        let mut stack = Vec::new();

        for name in self.nodes.keys() {
            self.visit_for_cycle(name, &mut visiting, &mut visited, &mut stack)?;
        }

        Ok(())
    }

    fn visit_for_cycle(
        &self,
        name: &str,
        visiting: &mut BTreeSet<String>,
        visited: &mut BTreeSet<String>,
        stack: &mut Vec<String>,
    ) -> anyhow::Result<()> {
        if visited.contains(name) {
            return Ok(());
        }
        if visiting.contains(name) {
            let start = stack
                .iter()
                .position(|component| component == name)
                .unwrap_or(0);
            let mut cycle = stack[start..].to_vec();
            cycle.push(name.to_string());
            return Err(anyhow!(
                "component dependency cycle detected: {}",
                cycle.join(" -> ")
            ));
        }

        visiting.insert(name.to_string());
        stack.push(name.to_string());
        if let Some(node) = self.nodes.get(name) {
            for dependency in &node.dependencies {
                self.visit_for_cycle(dependency, visiting, visited, stack)?;
            }
        }
        stack.pop();
        visiting.remove(name);
        visited.insert(name.to_string());
        Ok(())
    }
}

fn validate_component_identity(
    component: &ComponentRef,
    package: &PackageJson,
) -> anyhow::Result<()> {
    if let Some(declared) = &package.henosis.component {
        anyhow::ensure!(
            declared == &component.name,
            "package `{}` declares Henosis component `{declared}`, but candidate manifest entry is `{}`",
            package.name,
            component.name
        );
    }
    Ok(())
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

    fn package(name: &str, component: &str, deps: &[&str]) -> PackageJson {
        PackageJson {
            name: name.to_string(),
            dependencies: deps
                .iter()
                .map(|dep| (dep.to_string(), "workspace:*".to_string()))
                .collect(),
            henosis: PackageHenosis {
                component: Some(component.to_string()),
            },
        }
    }

    #[tokio::test]
    async fn closure_walks_all_transitive_reverse_dependents() {
        let reader = InMemoryPackageReader::new(vec![
            (
                "henosis-playground/service-a",
                "a-main",
                package(
                    "@henosis/service-a",
                    "service-a",
                    &["@henosis/platform-mock"],
                ),
            ),
            (
                "henosis-playground/service-b",
                "b-main",
                package(
                    "@henosis/service-b",
                    "service-b",
                    &["@henosis/platform-mock", "@henosis/service-a"],
                ),
            ),
            (
                "henosis-playground/service-c",
                "c-main",
                package(
                    "@henosis/service-c",
                    "service-c",
                    &["@henosis/platform-mock", "@henosis/service-b"],
                ),
            ),
        ]);
        let components = vec![
            ComponentRef::new("service-a", "henosis-playground/service-a", "a-main"),
            ComponentRef::new("service-b", "henosis-playground/service-b", "b-main"),
            ComponentRef::new("service-c", "henosis-playground/service-c", "c-main"),
        ];

        let graph = ComponentGraph::read(&components, &reader).await.unwrap();

        assert_eq!(
            graph.preview_closure(["service-a"]),
            BTreeSet::from([
                "service-a".to_string(),
                "service-b".to_string(),
                "service-c".to_string()
            ])
        );
        assert_eq!(
            graph.preview_closure(["service-b"]),
            BTreeSet::from(["service-b".to_string(), "service-c".to_string()])
        );
    }

    #[tokio::test]
    async fn ignores_non_manifest_henosis_package_dependencies() {
        let reader = InMemoryPackageReader::new(vec![
            (
                "henosis-playground/service-a",
                "a-main",
                package(
                    "@henosis/service-a",
                    "service-a",
                    &["@henosis/platform-mock", "@henosis/test-helper"],
                ),
            ),
            (
                "henosis-playground/service-b",
                "b-main",
                package("@henosis/service-b", "service-b", &[]),
            ),
        ]);
        let components = vec![
            ComponentRef::new("service-a", "henosis-playground/service-a", "a-main"),
            ComponentRef::new("service-b", "henosis-playground/service-b", "b-main"),
        ];

        let graph = ComponentGraph::read(&components, &reader).await.unwrap();

        assert_eq!(
            graph.node("service-a").unwrap().dependencies,
            BTreeSet::new()
        );
        assert_eq!(
            graph.preview_closure(["service-b"]),
            BTreeSet::from(["service-b".to_string()])
        );
    }

    #[tokio::test]
    async fn rejects_component_dependency_cycles() {
        let reader = InMemoryPackageReader::new(vec![
            (
                "henosis-playground/service-a",
                "a-main",
                package("@henosis/service-a", "service-a", &["@henosis/service-b"]),
            ),
            (
                "henosis-playground/service-b",
                "b-main",
                package("@henosis/service-b", "service-b", &["@henosis/service-a"]),
            ),
        ]);
        let components = vec![
            ComponentRef::new("service-a", "henosis-playground/service-a", "a-main"),
            ComponentRef::new("service-b", "henosis-playground/service-b", "b-main"),
        ];

        let error = ComponentGraph::read(&components, &reader)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("component dependency cycle detected"));
        assert!(error.contains("service-a"));
        assert!(error.contains("service-b"));
    }

    #[tokio::test]
    async fn rejects_package_identity_mismatches() {
        let reader = InMemoryPackageReader::new(vec![(
            "henosis-playground/service-a",
            "a-main",
            package("@henosis/service-a", "not-service-a", &[]),
        )]);
        let components = vec![ComponentRef::new(
            "service-a",
            "henosis-playground/service-a",
            "a-main",
        )];

        let error = ComponentGraph::read(&components, &reader)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("candidate manifest entry"));
    }
}
