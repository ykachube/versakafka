//! WAL reader

use crate::{WalError, Result, config::WalConfig};
use super::segment::{Segment, WalRecord, HEADER_SIZE, SEGMENT_MAGIC};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use bytes::Buf;
use crc32fast::Hasher;
use tracing::{debug, warn};

pub struct WalReader {
    segment: Segment,
    current_offset: i64,
}

impl WalReader {
    pub fn open(config: WalConfig, path: &Path) -> Result<Self> {
        let segment = Segment::open(config, path)?;
        let current_offset = segment.header.base_offset;
        Ok(Self { segment, current_offset })
    }

    pub fn read_all(&mut self) -> Result<Vec<WalRecord>> {
        let mut records = Vec::new();
        let mut file = self.segment.file.write();
        
        // Skip header
        file.seek(SeekFrom::Start(HEADER_SIZE))?;
        
        loop {
            match self.read_record(&mut file) {
                Ok(record) => {
                    self.current_offset = record.offset + 1;
                    records.push(record);
                }
                Err(e) => {
                    if e.to_string().contains("EOF") || e.to_string().contains("unexpected EOF") {
                        break;
                    }
                    return Err(e);
                }
            }
        }
        
        Ok(records)
    }
    
    pub fn read_from_offset(&mut self, offset: i64) -> Result<Vec<WalRecord>> {
        let mut records = Vec::new();
        let mut file = self.segment.file.write();
        
        // We need to scan from the beginning to find the offset
        // In a production system, we'd have an index
        file.seek(SeekFrom::Start(HEADER_SIZE))?;
        
        loop {
            match self.read_record(&mut file) {
                Ok(record) => {
                    if record.offset >= offset {
                        records.push(record);
                    }
                }
                Err(e) => {
                    if e.to_string().contains("EOF") || e.to_string().contains("unexpected EOF") {
                        break;
                    }
                    return Err(e);
                }
            }
        }
        
        Ok(records)
    }

    fn read_record(&self, file: &mut File) -> Result<WalRecord> {
        // Read record header (fixed size)
        // CRC(4) + Length(4) + Timestamp(8) + Offset(8) + KeyLen(4) + ValueLen(4) + HeadersCount(4) + EventIdLen(2) = 38
        let mut header_buf = [0u8; 38];
        
        match file.read_exact(&mut header_buf) {
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(WalError::Reader("EOF".to_string()));
            }
            Err(e) => return Err(WalError::Reader(e.to_string())),
        }
        
        let mut cursor = std::io::Cursor::new(&header_buf);
        let crc = cursor.get_u32();
        let length = cursor.get_u32();
        let timestamp = cursor.get_i64();
        let offset = cursor.get_i64();
        let key_length = cursor.get_i32();
        let value_length = cursor.get_i32();
        let headers_count = cursor.get_i32();
        let event_id_length = cursor.get_i16();
        
        // Read variable-length data
        let data_size = length as usize;
        if data_size < 38 {
            return Err(WalError::Reader("Invalid record length".to_string()));
        }
        
        let mut data = vec![0u8; data_size - 38];
        file.read_exact(&mut data)?;
        
        // Verify CRC
        let mut hasher = Hasher::new();
        hasher.update(&data);
        let computed_crc = hasher.finalize();
        
        if crc != computed_crc {
            return Err(WalError::ChecksumMismatch(format!("CRC mismatch: expected {}, got {}", crc, computed_crc)));
        }
        
        let mut data_cursor = std::io::Cursor::new(&data);
        
        // Key
        let key = if key_length >= 0 {
            let mut key_bytes = vec![0u8; key_length as usize];
            data_cursor.read_exact(&mut key_bytes)?;
            Some(key_bytes)
        } else {
            None
        };
        
        // Value
        let mut value = vec![0u8; value_length as usize];
        data_cursor.read_exact(&mut value)?;
        
        // Headers
        let mut headers = Vec::new();
        for _ in 0..headers_count {
            let key_len = data_cursor.get_i16() as usize;
            let mut key = vec![0u8; key_len];
            data_cursor.read_exact(&mut key)?;
            let key_str = String::from_utf8(key).map_err(|e| WalError::Reader(e.to_string()))?;
            
            let value_len = data_cursor.get_i32() as usize;
            let mut value = vec![0u8; value_len];
            data_cursor.read_exact(&mut value)?;
            
            headers.push((key_str, value));
        }
        
        // Event ID
        let event_id = if event_id_length > 0 {
            let mut eid = vec![0u8; event_id_length as usize];
            data_cursor.read_exact(&mut eid)?;
            Some(eid)
        } else {
            None
        };
        
        Ok(WalRecord {
            timestamp,
            key,
            value,
            headers,
            event_id,
            offset,
        })
    }
}