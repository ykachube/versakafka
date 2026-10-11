//! Crypto headers

use crate::CryptoError;
use super::config::CryptoHeaders;

pub fn add_crypto_headers(
    headers: &mut Vec<(String, Vec<u8>)>,
    key_id: &str,
    nonce: &[u8],
    tag: &[u8],
    algorithm: &str,
    config: &CryptoHeaders,
) {
    if config.key_id {
        headers.push(("encryption-key-id".to_string(), key_id.as_bytes().to_vec()));
    }
    if config.iv {
        headers.push(("encryption-iv".to_string(), nonce.to_vec()));
    }
    if config.tag {
        headers.push(("encryption-tag".to_string(), tag.to_vec()));
    }
    if config.algorithm {
        headers.push(("encryption-algorithm".to_string(), algorithm.as_bytes().to_vec()));
    }
}