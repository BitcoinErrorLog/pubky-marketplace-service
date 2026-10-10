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
    track_git_head();
    git(&["rev-parse", "HEAD"])
}

/// Rebuilds when HEAD moves: HEAD itself, the branch it points at, and
/// packed-refs (where a packed branch lives).
fn track_git_head() {
    let mut paths = vec!["HEAD".to_owned(), "packed-refs".to_owned()];
    if let Some(branch) = git(&["symbolic-ref", "-q", "HEAD"]) {
        paths.push(branch);
    }
    for path in paths {
        if let Some(file) = git(&["rev-parse", "--path-format=absolute", "--git-path", &path]) {
            if std::path::Path::new(&file).exists() {
                println!("cargo:rerun-if-changed={file}");
            }
        }
    }
}

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!value.is_empty()).then_some(value)
}
