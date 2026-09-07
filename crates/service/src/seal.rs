//! Shared XChaCha20-Poly1305 seal/open helpers.
//!
//! Extracted from `locks.rs` so every secret the service holds at rest —
//! Locks bundle ids, local-pickup details, and pinned payment snapshots —
//! uses one construction: a random 24-byte nonce prepended to the
//! ciphertext, AAD binding the ciphertext to its owning row so it cannot be
//! transplanted, and no serialization path for the plaintext.

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use rand::RngCore;

pub const XNONCE_LEN: usize = 24;
pub const KEY_LEN: usize = 32;

/// Seals `plaintext` under `key` with `aad` as associated data: a fresh
/// random 24-byte nonce followed by the XChaCha20-Poly1305 ciphertext.
pub fn seal(key: &[u8; KEY_LEN], aad: &[u8], plaintext: &[u8]) -> Vec<u8> {
    let cipher = XChaCha20Poly1305::new(key.into());
    let mut nonce_bytes = [0u8; XNONCE_LEN];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let nonce = XNonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, Payload { msg: plaintext, aad })
        .expect("XChaCha20-Poly1305 encryption is infallible for in-memory buffers");
    let mut sealed = Vec::with_capacity(XNONCE_LEN + ciphertext.len());
    sealed.extend_from_slice(&nonce_bytes);
    sealed.extend_from_slice(&ciphertext);
    sealed
}

/// Opens a ciphertext produced by [`seal`]. Fails when the ciphertext was
/// not produced under this key with this AAD (a wrong key, a transplanted
/// row, or tampering).
pub fn open(key: &[u8; KEY_LEN], aad: &[u8], sealed: &[u8]) -> anyhow::Result<Vec<u8>> {
    if sealed.len() <= XNONCE_LEN {
        anyhow::bail!("sealed payload is too short");
    }
    let (nonce_bytes, ciphertext) = sealed.split_at(XNONCE_LEN);
    let cipher = XChaCha20Poly1305::new(key.into());
    cipher
        .decrypt(
            XNonce::from_slice(nonce_bytes),
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map_err(|_| anyhow::anyhow!("ciphertext did not authenticate under the configured key"))
}

/// Parses a 32-byte key from its 64-character hex env-var form.
pub fn parse_key(name: &str, hex_value: &str) -> anyhow::Result<[u8; KEY_LEN]> {
    let bytes = hex::decode(hex_value.trim())
        .map_err(|_| anyhow::anyhow!("{name} must be 64 hexadecimal characters"))?;
    <[u8; KEY_LEN]>::try_from(bytes)
        .map_err(|_| anyhow::anyhow!("{name} must decode to exactly 32 bytes"))
}
