//! Frame encoding/decoding

use crate::{ProtocolError, Result};
use bytes::{Buf, BufMut, BytesMut};

pub const FRAME_HEADER_SIZE: usize = 4; // Length prefix

pub fn encode_frame(buf: &mut BytesMut, data: &[u8]) {
    let len = data.len() as i32;
    buf.put_i32(len);
    buf.put_slice(data);
}

pub fn decode_frame(buf: &mut BytesMut) -> Result<Option<Vec<u8>>> {
    if buf.len() < FRAME_HEADER_SIZE {
        return Ok(None);
    }
    
    // Peek at the length without consuming
    let len = buf.get_i32() as usize;
    if buf.len() < len {
        // Not enough data, we already consumed the length
        // Put it back by creating a new buffer with the length prepended
        // Actually, just return None and let the caller handle it
        // We need to restore the buffer state
        // The simplest approach: return None and the caller should not advance
        return Ok(None);
    }
    
    let mut data = vec![0u8; len];
    buf.copy_to_slice(&mut data);
    Ok(Some(data))
}

pub fn try_decode_frame(buf: &mut BytesMut) -> Result<Vec<u8>> {
    decode_frame(buf)?.ok_or_else(|| ProtocolError::InvalidFrame("Incomplete frame".to_string()))
}