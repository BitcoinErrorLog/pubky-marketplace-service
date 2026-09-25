//! Operator commands for released Paykit attempts held for review
//! (runbook: `docs/paykit-released-attempts.md`).
//!
//! ```text
//! paykit-attempts-admin list
//! paykit-attempts-admin resolve <order_id> <invoice_id> refunded <refund reference>
//! paykit-attempts-admin resolve <order_id> <invoice_id> dismissed <reason>
//! ```
//!
//! `DATABASE_URL` names the database; `PAYKIT_ADMIN_OPERATOR` names the
//! operator recorded on a resolution.

use marketplace_service::paykit_attempts::{
    list_needs_review, resolve_needs_review, ReviewOutcome,
};
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let database_url =
        std::env::var("DATABASE_URL").map_err(|_| anyhow::anyhow!("DATABASE_URL is required"))?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await?;
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["list"] => {
            for row in list_needs_review(&pool).await? {
                println!("{}", serde_json::to_string(&row)?);
            }
            Ok(())
        }
        ["resolve", order_id, invoice_id, outcome, note] => {
            let operator = std::env::var("PAYKIT_ADMIN_OPERATOR")
                .map_err(|_| anyhow::anyhow!("PAYKIT_ADMIN_OPERATOR is required"))?;
            let resolved = resolve_needs_review(
                &pool,
                order_id.parse::<Uuid>()?,
                invoice_id.parse::<Uuid>()?,
                ReviewOutcome::parse(outcome)?,
                note,
                &operator,
                chrono::Utc::now(),
            )
            .await?;
            if !resolved {
                anyhow::bail!(
                    "no released attempt with that order and invoice is waiting for review"
                );
            }
            println!("resolved");
            Ok(())
        }
        _ => anyhow::bail!(
            "usage: paykit-attempts-admin list | resolve <order_id> <invoice_id> \
             <refunded|dismissed> <note>"
        ),
    }
}
