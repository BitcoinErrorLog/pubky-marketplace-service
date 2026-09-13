//! A local double for the bounded sole FX source (Blocktank's BTCUSD
//! ticker): serves one scripted body, counts fetches, and lets a test force
//! transport failures — the feed-side counterpart of [`super::FakePaykit`].
//! The marketplace's own FX wrapper is never mocked; the double speaks the
//! pinned wire contract from `tests/fixtures/blocktank_fx_rates_btc.json`.

use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::StatusCode;
use axum::Router;

#[derive(Clone)]
struct FakeFxState {
    inner: Arc<Mutex<FakeFxInner>>,
}

struct FakeFxInner {
    status: u16,
    body: String,
    fetches: usize,
}

/// A local FX feed double. `fetches` is the fetch counter the idempotency
/// tests assert against.
pub struct FakeFxFeed {
    state: FakeFxState,
    pub base_url: String,
}

impl FakeFxFeed {
    /// Serves the given body at 200 for every later fetch.
    pub fn set_body(&self, body: String) {
        let mut guard = self.state.inner.lock().expect("fake fx lock");
        guard.status = 200;
        guard.body = body;
    }

    /// Forces a transport-level failure (a 500 the client maps to its
    /// malformed/unavailable class) for every later fetch.
    pub fn set_status(&self, status: u16) {
        self.state.inner.lock().expect("fake fx lock").status = status;
    }

    pub fn fetch_count(&self) -> usize {
        self.state.inner.lock().expect("fake fx lock").fetches
    }
}

/// The pinned Blocktank wire shape with one BTCUSD ticker.
pub fn fx_body(price: &str, last_updated_millis: i64) -> String {
    format!(
        r#"{{"tickers":[{{"symbol":"BTCUSD","lastPrice":"{price}","base":"BTC","quote":"USD","lastUpdatedAt":{last_updated_millis}}}]}}"#
    )
}

async fn serve_fx_rates(State(state): State<FakeFxState>) -> (StatusCode, String) {
    let mut guard = state.inner.lock().expect("fake fx lock");
    guard.fetches += 1;
    (
        StatusCode::from_u16(guard.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        guard.body.clone(),
    )
}

pub async fn spawn_fake_fx() -> FakeFxFeed {
    let state = FakeFxState {
        inner: Arc::new(Mutex::new(FakeFxInner {
            status: 200,
            body: String::new(),
            fetches: 0,
        })),
    };
    let router = Router::new()
        .route("/api/fx/rates/btc", axum::routing::get(serve_fx_rates))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake fx binds");
    let base_url = format!("http://{}/api/fx/rates/btc", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("fake fx serves");
    });
    FakeFxFeed { state, base_url }
}
