//! Bakes the build's commit and time into the binary for `GET /version`.

use std::process::Command;

fn main() {
    for var in ["GIT_COMMIT", "RAILWAY_GIT_COMMIT_SHA", "BUILD_TIME"] {
        println!("cargo:rerun-if-env-changed={var}");
    }

    let commit = env_value("GIT_COMMIT")
        .or_else(|| env_value("RAILWAY_GIT_COMMIT_SHA"))
        .or_else(git_head)
        .unwrap_or_else(|| "unknown".to_owned());
    let built_at = env_value("BUILD_TIME").unwrap_or_else(|| "unknown".to_owned());

    println!("cargo:rustc-env=MARKETPLACE_GIT_COMMIT={commit}");
    println!("cargo:rustc-env=MARKETPLACE_BUILT_AT={built_at}");
}

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn git_head() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!sha.is_empty()).then_some(sha)
}
