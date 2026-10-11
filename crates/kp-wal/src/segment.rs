//! WAL segment

use crate::{WalError, Result, config::WalConfig};
use bytes::{Buf, BufMut, BytesMut};
use crc32fast::Hasher;
use std::fs::File;
use std::io::{Write, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use parking_lot::RwLock;

pub const SEGMENT_MAGIC: u32 = 0x4B50574C; // "KPWL"
pub const SEGMENT_VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub struct SegmentHeader {
    pub magic: u32,
    pub version: u16,
    pub segment_id: u64,
    pub created_at: i64,
    pub base_offset: i64,
}

#[derive(Debug, Clone)]
pub struct RecordHeader {
    pub crc32: u32,
    pub length: u32,
    pub timestamp: i64,
    pub offset: i64,
    pub key_length: i32,
    pub value_length: i32,
    pub headers_count: i32,
    pub event_id_length: i16,
}

pub struct Segment {
    pub config: WalConfig,
    pub path: PathBuf,
    pub file: Arc<RwLock<File>>,
    pub header: SegmentHeader,
    pub next_offset: i64,
    pub size: u64,
}

impl Segment {
    pub fn create(config: WalConfig, dir: &Path, segment_id: u64, base_offset: i64) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(format!("{:020}.wal", segment_id));
        
        let file = File::create(&path)?;
        let mut segment = Self {
            config,
            path,
            file: Arc::new(RwLock::new(file)),
            header: SegmentHeader {
                magic: SEGMENT_MAGIC,
                version: SEGMENT_VERSION,
                segment_id,
                created_at: chrono::Utc::now().timestamp_millis(),
                base_offset,
            },
            next_offset: base_offset,
            size: 0,
        };
        
        segment.write_header()?;
        Ok(segment)
    }

    pub fn open(config: WalConfig, path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
        let header = Self::read_header(&mut file)?;
        let size = file.metadata()?.len();
        let next_offset = header.base_offset + 1; // Simplified
        
        Ok(Self {
            config,
            path: path.to_path_buf(),
            file: Arc::new(RwLock::new(file)),
            header,
            next_offset,
            size,
        })
    }

    fn write_header(&mut self) -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_u32(self.header.magic);
        buf.put_u16(self.header.version);
        buf.put_u64(self.header.segment_id);
        buf.put_i64(self.header.created_at);
        buf.put_i64(self.header.base_offset);
        
        self.file.write().write_all(&buf)?;
        self.size += buf.len() as u64;
        Ok(())
    }

    fn read_header(file: &mut File) -> Result<SegmentHeader> {
        let mut buf = [0u8; 26]; // 4+2+8+8+8
        file.read_exact(&mut buf)?;
        let mut cursor = std::io::Cursor::new(&buf);
        
        Ok(SegmentHeader {
            magic: cursor.get_u32(),
            version: cursor.get_u16(),
            segment_id: cursor.get_u64(),
            created_at: cursor.get_i64(),
            base_offset: cursor.get_i64(),
        })
    }

    pub fn append(&mut self, record: &WalRecord) -> Result<i64> {
        let offset = self.next_offset;
        self.next_offset += 1;
        
        // Serialize record
        let mut buf = BytesMut::new();
        
        // Record header placeholder
        let header_start = buf.len();
        buf.put_u32(0); // CRC placeholder
        buf.put_u32(0); // Length placeholder
        buf.put_i64(record.timestamp);
        buf.put_i64(offset);
        buf.put_i32(record.key.as_ref().map(|k| k.len() as i32).unwrap_or(-1));
        buf.put_i32(record.value.len() as i32);
        buf.put_i32(record.headers.len() as i32);
        buf.put_i16(record.event_id.as_ref().map(|e| e.len() as i16).unwrap_or(0));
        
        // Key
        if let Some(key) = &record.key {
            buf.put_slice(key);
        }
        
        // Value
        buf.put_slice(&record.value);
        
        // Headers
        for (k, v) in &record.headers {
            buf.put_i16(k.len() as i16);
            buf.put_slice(k.as_bytes());
            buf.put_i32(v.len() as i32);
            buf.put_slice(v);
        }
        
        // Event ID
        if let Some(eid) = &record.event_id {
            buf.put_slice(eid);
        }
        
        // Compute CRC
        let record_data = &buf[header_start + 8..]; // Skip CRC and length
        let mut hasher = Hasher::new();
        hasher.update(record_data);
        let crc = hasher.finalize();
        
        // Write CRC and length
        let record_len = (buf.len() - header_start) as u32;
        buf[header_start..header_start+4].copy_from_slice(&crc.to_be_bytes());
        buf[header_start+4..header_start+8].copy_from_slice(&record_len.to_be_bytes());
        
        // Write to file
        let mut file = self.file.write();
        file.write_all(&buf)?;
        
        // Fsync if configured
        if matches!(self.config.fsync_policy, super::config::FsyncPolicy::Always) {
            file.flush()?;
            file.sync_all()?;
        }
        
        self.size += buf.len() as u64;
        Ok(offset)
    }

    pub fn is_full(&self) -> bool {
        self.size >= self.config.segment_size as u64
    }
}

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub timestamp: i64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub event_id: Option<Vec<u8>>,
}