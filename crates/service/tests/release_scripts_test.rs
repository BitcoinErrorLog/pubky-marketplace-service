//! The production-schema clone rehearsal's targeting contract
//! (`scripts/release/rehearse-0042-production-clone.sh`).

use std::io::Write;
use std::process::{Command, Stdio};

const ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
const PROJECT: &str = "75faa4fe-466c-4277-977f-1d8e4e31df8c";
const ENVIRONMENT: &str = "404919ad-fb95-4621-9f45-b7f993dfa8ae";
const POSTGRES: &str = "6833471b-a73c-403f-a7c4-64a26dc8e155";

/// The shape `railway status --project … --environment … --json` returns,
/// reduced to the fields the target check reads.
fn status() -> String {
    serde_json::json!({
        "id": PROJECT,
        "environments": { "edges": [ { "node": { "id": ENVIRONMENT, "name": "production" } } ] },
        "services": { "edges": [
            { "node": { "id": "a91a9661-080f-4c39-ad64-da376ff065b9", "name": "marketplace-service" } },
            { "node": { "id": POSTGRES, "name": "Postgres" } },
            { "node": { "id": "83e0eab5-e312-458e-b923-8867bbf7c3b5", "name": "paykit-postgres" } }
        ] }
    })
    .to_string()
}

fn target(args: [&str; 4]) -> bool {
    let mut child = Command::new("python3")
        .arg(format!("{ROOT}/scripts/release/railway_target.py"))
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("python3 runs");
    child
        .stdin
        .take()
        .expect("stdin")
        .write_all(status().as_bytes())
        .expect("status written");
    child.wait().expect("target check exits").success()
}

#[test]
fn the_target_check_accepts_only_the_expected_service() {
    assert!(target([PROJECT, ENVIRONMENT, "Postgres", POSTGRES]));
    assert!(
        !target([
            PROJECT,
            ENVIRONMENT,
            "Postgres",
            "83e0eab5-e312-458e-b923-8867bbf7c3b5"
        ]),
        "a service whose id is not the expected one"
    );
    assert!(
        !target([PROJECT, ENVIRONMENT, POSTGRES, POSTGRES]),
        "a service id where `railway connect` needs the name"
    );
    assert!(
        !target([
            PROJECT,
            "c67a6435-bb23-453b-9169-764bfa0312e1",
            "Postgres",
            POSTGRES
        ]),
        "an environment outside the project"
    );
    assert!(
        !target([
            "c991d768-4a3c-42ea-b5ed-eaa22d4916ed",
            ENVIRONMENT,
            "Postgres",
            POSTGRES
        ]),
        "another project"
    );
}

#[test]
fn the_rehearsal_is_executable_and_verifies_its_target_before_connecting() {
    let path = format!("{ROOT}/scripts/release/rehearse-0042-production-clone.sh");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path)
            .expect("rehearsal script")
            .permissions()
            .mode();
        assert_ne!(mode & 0o111, 0, "the rehearsal script is executable");
    }
    let script = std::fs::read_to_string(&path).expect("rehearsal script");
    let check = script
        .find("scripts/release/railway_target.py")
        .expect("the script runs the target check");
    let connect = script
        .find("railway connect \"$RAILWAY_DATABASE_SERVICE\"")
        .expect("the script opens the tunnel");
    assert!(
        check < connect,
        "the target is verified before the tunnel opens"
    );
    assert!(script.contains("RAILWAY_DATABASE_SERVICE_ID:?"));
}
