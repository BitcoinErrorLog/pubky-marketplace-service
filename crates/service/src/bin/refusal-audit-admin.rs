use std::io;

use marketplace_service::refusal_audit_admin::{run, AdminConfig, AdminOperation};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args();
    let _binary = args.next();
    let operation = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("one operation is required"))
        .and_then(|value| AdminOperation::parse(&value))?;
    if args.next().is_some() {
        anyhow::bail!("only one operation argument is accepted");
    }
    run(AdminConfig::from_env()?, operation, io::stdin().lock()).await
}
