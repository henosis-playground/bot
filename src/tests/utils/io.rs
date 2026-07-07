use std::path::Path;

use anyhow::Context;
use serde::Deserialize;
use serde_json::Value;

const ROOT_DIR: &str = env!("CARGO_MANIFEST_DIR");

pub fn load_test_file(path: &str) -> String {
    let path = Path::new(ROOT_DIR).join("tests").join("data").join(path);
    std::fs::read_to_string(path).unwrap()
}

#[derive(Deserialize)]
struct RecordedWebhook {
    headers: std::collections::BTreeMap<String, Value>,
    body: Value,
}

pub fn load_recorded_webhook(path: &str) -> anyhow::Result<(String, Vec<u8>)> {
    let path = Path::new(ROOT_DIR).join("tests").join("data").join(path);
    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("Cannot read webhook fixture {}", path.display()))?;
    let envelope: RecordedWebhook = serde_json::from_str(&content)
        .with_context(|| format!("Cannot parse webhook fixture {}", path.display()))?;

    let event = envelope
        .headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("x-github-event"))
        .and_then(|(_, value)| value.as_str())
        .context("Recorded webhook fixture is missing x-github-event")?
        .to_string();
    let body = serde_json::to_vec(&envelope.body).context("Cannot serialize webhook body")?;

    Ok((event, body))
}
