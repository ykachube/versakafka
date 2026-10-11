//! Kafka protocol codec

use crate::{ProtocolError, Result};
use kafka_protocol::protocol::{Decodable, Encodable};
use bytes::BytesMut;

pub fn encode<T: Encodable>(value: &T, buf: &mut BytesMut, version: i16) -> Result<()> {
    value.encode(buf, version).map_err(|e| ProtocolError::Encode(e.to_string()))
}

pub fn decode<T: Decodable>(buf: &mut BytesMut, version: i16) -> Result<T> {
    T::decode(buf, version).map_err(|e| ProtocolError::Decode(e.to_string()))
}

// Default version (V0)
pub fn encode_default<T: Encodable>(value: &T, buf: &mut BytesMut) -> Result<()> {
    encode(value, buf, 0)
}

pub fn decode_default<T: Decodable>(buf: &mut BytesMut) -> Result<T> {
    decode(buf, 0)
}