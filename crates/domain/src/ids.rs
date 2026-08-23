use uuid::Uuid;

pub fn listing_aggregate_id(seller_pubky: &str, listing_id: &str) -> String {
    format!("listing:{seller_pubky}_{listing_id}")
}

pub fn drop_aggregate_id(seller_pubky: &str, drop_id: &str) -> String {
    format!("drop:{seller_pubky}_{drop_id}")
}

pub fn checkout_aggregate_id(command_id: Uuid) -> String {
    format!("checkout:{command_id}")
}

pub fn order_aggregate_id(order_id: Uuid) -> String {
    format!("order:{order_id}")
}

pub fn payment_aggregate_id(payment_id: Uuid) -> String {
    format!("payment:{payment_id}")
}

pub fn offer_aggregate_id(offer_id: Uuid) -> String {
    format!("offer:{offer_id}")
}

pub fn report_aggregate_id(report_id: Uuid) -> String {
    format!("report:{report_id}")
}

/// Aggregate for a seller's own marketplace settings (band consent).
pub fn seller_settings_aggregate_id(seller_pubky: &str) -> String {
    format!("seller_settings:{seller_pubky}")
}
