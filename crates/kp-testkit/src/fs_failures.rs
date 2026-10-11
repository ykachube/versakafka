//! File system failure injection for testing

use crate::TestKitError;
use std::sync::Arc;
use parking_lot::RwLock;

pub struct FsFailures {
    state: Arc<RwLock<FsState>>,
}

#[derive(Default)]
struct FsState {
    fail_writes: bool,
    fail_fsync: bool,
    fail_reads: bool,
    disk_full: bool,
}

impl FsFailures {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(FsState::default())),
        }
    }

    pub fn enable_write_failure(&self) {
        self.state.write().fail_writes = true;
    }

    pub fn disable_write_failure(&self) {
        self.state.write().fail_writes = false;
    }

    pub fn enable_fsync_failure(&self) {
        self.state.write().fail_fsync = true;
    }

    pub fn enable_disk_full(&self) {
        self.state.write().disk_full = true;
    }

    pub fn check_write(&self) -> Result<(), TestKitError> {
        if self.state.read().fail_writes {
            return Err(TestKitError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Injected write failure",
            )));
        }
        if self.state.read().disk_full {
            return Err(TestKitError::Io(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                "Injected disk full",
            )));
        }
        Ok(())
    }

    pub fn check_fsync(&self) -> Result<(), TestKitError> {
        if self.state.read().fail_fsync {
            return Err(TestKitError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Injected fsync failure",
            )));
        }
        Ok(())
    }
}