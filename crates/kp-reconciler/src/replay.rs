//! Gap replay

use crate::{ReconcilerError, Result, config::ReconcilerConfig};
use kp_spill::replayer::Replayer;
use kp_channel::Channel;

pub struct GapReplayer {
    config: ReconcilerConfig,
}

impl GapReplayer {
    pub fn new(config: ReconcilerConfig) -> Self {
        Self { config }
    }

    pub async fn replay_gaps(&self, gaps: &[super::checker::Gap], replayer: &Replayer, channel: &dyn Channel) -> Result<()> {
        // TODO: Implement gap replay
        // For each gap, read from WAL/S3 and replay to channel
        
        Ok(())
    }
}