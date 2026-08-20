use std::path::PathBuf;

fn main() {
    let path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../contracts/state-machines.json")
        });
    let rendered = marketplace_domain::state_machines::contract_json_pretty();
    std::fs::write(&path, rendered).expect("failed to write state machine contract");
    println!("wrote {}", path.display());
}
