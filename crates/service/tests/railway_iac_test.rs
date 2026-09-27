//! Railway config-as-code deletes every variable its env objects omit, so an
//! apply that omits a sealing key deletes it, and every row sealed under it
//! stops opening. Every key-material variable the service reads must be a
//! `preserve()` entry in each graph that can be applied: production and
//! staging in `.railway/railway.ts`, and staging in `.railway/railway.staging.ts`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Suffixes of variables that hold key material, or the epoch that selects
/// it. A `_PREVIOUS` rotation partner matches through the same suffixes.
const KEY_MATERIAL_SUFFIXES: [&str; 8] = [
    "ENCRYPTION_KEY",
    "HMAC_KEY",
    "ROOT_B64",
    "KEY_B64",
    "SECRET_KEY",
    "SIGNING_KEY",
    "_SALT",
    "KEY_EPOCH",
];

fn is_key_material(name: &str) -> bool {
    let base = name.strip_suffix("_PREVIOUS").unwrap_or(name);
    KEY_MATERIAL_SUFFIXES
        .iter()
        .any(|suffix| base.ends_with(suffix))
}

fn is_env_name(token: &str) -> bool {
    token.len() > 3
        && token.starts_with(|c: char| c.is_ascii_uppercase())
        && token
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("source directory reads") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// Every quoted upper-case string literal in the service source that names
/// key material.
fn key_material_names_in_source() -> BTreeSet<String> {
    let mut files = Vec::new();
    rust_sources(&repository_root().join("crates/service/src"), &mut files);
    let mut names = BTreeSet::new();
    for file in files {
        let source = std::fs::read_to_string(&file).expect("source reads");
        let mut rest = source.as_str();
        while let Some(open) = rest.find('"') {
            rest = &rest[open + 1..];
            let token_end = rest
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .unwrap_or(rest.len());
            let token = &rest[..token_end];
            if rest[token_end..].starts_with('"') && is_env_name(token) && is_key_material(token) {
                names.insert(token.to_string());
            }
        }
    }
    names
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Entry {
    Spread(String),
    Var { name: String, preserved: bool },
}

/// The `const <name> = { ... };` object literals of a Railway IaC file.
fn env_objects(source: &str) -> BTreeMap<String, Vec<Entry>> {
    let mut objects = BTreeMap::new();
    let mut current: Option<(String, Vec<Entry>)> = None;
    for line in source.lines() {
        let trimmed = line.trim();
        if let Some((name, entries)) = current.as_mut() {
            if trimmed == "};" {
                objects.insert(name.clone(), std::mem::take(entries));
                current = None;
            } else if let Some(spread) = trimmed.strip_prefix("...") {
                entries.push(Entry::Spread(spread.trim_end_matches(',').to_string()));
            } else if let Some((key, value)) = trimmed.split_once(':') {
                if is_env_name(key) {
                    entries.push(Entry::Var {
                        name: key.to_string(),
                        preserved: value.trim() == "preserve(),",
                    });
                }
            }
        } else if let Some(rest) = trimmed.strip_prefix("const ") {
            if let Some(name) = rest.strip_suffix(" = {") {
                current = Some((name.to_string(), Vec::new()));
            }
        }
    }
    objects
}

/// The variables of one env object after its spreads, later entries winning.
fn resolved(objects: &BTreeMap<String, Vec<Entry>>, name: &str) -> BTreeMap<String, bool> {
    let entries = objects
        .get(name)
        .unwrap_or_else(|| panic!("env object `{name}` exists"));
    let mut vars = BTreeMap::new();
    for entry in entries {
        match entry {
            Entry::Spread(other) => vars.extend(resolved(objects, other)),
            Entry::Var { name, preserved } => {
                vars.insert(name.clone(), *preserved);
            }
        }
    }
    vars
}

fn unpreserved<'a>(
    graph: &BTreeMap<String, bool>,
    required: &'a BTreeSet<String>,
) -> Vec<&'a String> {
    required
        .iter()
        .filter(|name| graph.get(*name) != Some(&true))
        .collect()
}

fn graphs() -> Vec<(&'static str, BTreeMap<String, bool>)> {
    let root = repository_root();
    let dual = env_objects(
        &std::fs::read_to_string(root.join(".railway/railway.ts")).expect("railway.ts reads"),
    );
    let staging_only = env_objects(
        &std::fs::read_to_string(root.join(".railway/railway.staging.ts"))
            .expect("railway.staging.ts reads"),
    );
    vec![
        ("railway.ts production", resolved(&dual, "productionEnv")),
        ("railway.ts staging", resolved(&dual, "stagingEnv")),
        (
            "railway.staging.ts staging",
            resolved(&staging_only, "stagingEnv"),
        ),
    ]
}

#[test]
fn every_sealing_key_the_service_reads_is_preserved_in_every_railway_graph() {
    let required = key_material_names_in_source();
    for known in [
        "PRIV_DATA_KEY_ENCRYPTION_KEY",
        "PRIV_DATA_KEY_ENCRYPTION_KEY_PREVIOUS",
        "DIGITAL_DELIVERY_ENCRYPTION_KEY",
        "PICKUP_DETAILS_ENCRYPTION_KEY_PREVIOUS",
        "LOCKS_BUNDLE_ENCRYPTION_KEY",
        "GRANT_FLOW_PREVIOUS_KEY_EPOCH",
    ] {
        assert!(
            required.contains(known),
            "the source scan finds {known}; found {required:?}"
        );
    }
    for (label, graph) in graphs() {
        assert!(
            graph.len() > required.len(),
            "{label} parsed to {} variables",
            graph.len()
        );
        let missing = unpreserved(&graph, &required);
        assert!(
            missing.is_empty(),
            "{label} does not preserve() {missing:?}; a config apply would delete them"
        );
    }
}

#[test]
fn the_check_flags_an_omitted_or_overwritten_sealing_key() {
    let source = r#"
const sharedEnv = {
  DATABASE_URL: preserve(),
  PRIV_DATA_KEY_ENCRYPTION_KEY: preserve(),
};
const productionEnv = {
  ...sharedEnv,
  PICKUP_DETAILS_ENCRYPTION_KEY: "literal",
};
"#;
    let objects = env_objects(source);
    let production = resolved(&objects, "productionEnv");
    let required: BTreeSet<String> = [
        "PRIV_DATA_KEY_ENCRYPTION_KEY",
        "PRIV_DATA_KEY_ENCRYPTION_KEY_PREVIOUS",
        "PICKUP_DETAILS_ENCRYPTION_KEY",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    assert_eq!(
        unpreserved(&production, &required),
        vec![
            "PICKUP_DETAILS_ENCRYPTION_KEY",
            "PRIV_DATA_KEY_ENCRYPTION_KEY_PREVIOUS"
        ]
    );
    assert!(is_key_material("GRANT_RESULT_HMAC_PREVIOUS_ROOT_B64"));
    assert!(is_key_material("DIGITAL_DELIVERY_ENCRYPTION_KEY_PREVIOUS"));
    assert!(!is_key_material("DATABASE_URL"));
    assert!(!is_key_material("PAYKIT_SERVER_URL"));
}
