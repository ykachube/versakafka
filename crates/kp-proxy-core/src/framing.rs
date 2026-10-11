//! Framing for Kafka protocol

use crate::{ProxyCoreError, Result};
use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};
use kp_protocol::frame::{encode_frame, decode_frame};

pub struct KafkaFrameCodec;

impl Decoder for KafkaFrameCodec {
    type Item = Vec<u8>;
    type Error = ProxyCoreError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        decode_frame(src).map_err(|e| ProxyCoreError::Framing(e.to_string()))
    }
}

impl Encoder<Vec<u8>> for KafkaFrameCodec {
    type Error = ProxyCoreError;

    fn encode(&mut self, item: Vec<u8>, dst: &mut BytesMut) -> Result<()> {
        encode_frame(dst, &item);
        Ok(())
    }
}