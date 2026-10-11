//! Event ID headers

use crate::EventIdMode;
use super::config::EventIdHeaders;

pub fn add_event_id_headers(
    headers: &mut Vec<(String, Vec<u8>)>,
    event_id: &[u8],
    config: &EventIdHeaders,
    region: &str,
    proxy_id: &str,
    channel: &str,
) {
    if config.event_id {
        headers.push(("event-id".to_string(), event_id.to_vec()));
    }
    if config.ingest_ts {
        let ts = chrono::Utc::now().timestamp_millis().to_be_bytes().to_vec();
        headers.push(("ingest-ts".to_string(), ts));
    }
    if config.proxy_region {
        headers.push(("proxy-region".to_string(), region.as_bytes().to_vec()));
    }
    if config.proxy_id {
        headers.push(("proxy-id".to_string(), proxy_id.as_bytes().to_vec()));
    }
    if config.channel {
        headers.push(("channel".to_string(), channel.as_bytes().to_vec()));
    }
}