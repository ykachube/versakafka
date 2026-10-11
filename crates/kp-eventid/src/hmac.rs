//! HMAC for event ID tamper evidence

use crate::EventIdError;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn compute_hmac(key: &[u8], event_id: &[u8]) -> Result<Vec<u8>, EventIdError> {
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|e| EventIdError::Hmac(e.to_string()))?;
    mac.update(event_id);
    Ok(mac.finalize().into_bytes().to_vec())
}

pub fn verify_hmac(key: &[u8], event_id: &[u8], expected: &[u8]) -> Result<bool, EventIdError> {
    let computed = compute_hmac(key, event_id)?;
    Ok(computed.as_slice() == expected)
}