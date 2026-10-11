//! Filter hooks

use crate::{FilterError, chain::{FilterBatch, FilterRecord}};
use kp_protocol::{ProtocolError, Result as ProtocolResult};

pub trait PreReceiveHook: Send + Sync {
    fn on_pre_receive(&self, data: &mut [u8]) -> Result<(), FilterError>;
}

pub trait PostReceiveHook: Send + Sync {
    fn on_post_receive(&self, batch: &mut FilterBatch) -> Result<(), FilterError>;
}

pub trait PreSendHook: Send + Sync {
    fn on_pre_send(&self, batch: &mut FilterBatch) -> Result<(), FilterError>;
}

pub trait PostSendHook: Send + Sync {
    fn on_post_send(&self, batch: &FilterBatch, result: &SendResult) -> Result<(), FilterError>;
}

pub struct SendResult {
    pub success: bool,
    pub error: Option<String>,
    pub offset: Option<i64>,
}