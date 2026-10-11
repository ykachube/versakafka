//! Built-in filters

use crate::{FilterError, chain::{FilterStage, FilterBatch, FilterRecord}};

pub struct EnrichmentFilter;
pub struct FilteringFilter;

impl FilterStage for EnrichmentFilter {
    fn name(&self) -> &str { "enrichment" }
    fn process_batch(&self, _batch: &mut FilterBatch) -> Result<(), FilterError> { Ok(()) }
}

impl FilterStage for FilteringFilter {
    fn name(&self) -> &str { "filtering" }
    fn process_batch(&self, _batch: &mut FilterBatch) -> Result<(), FilterError> { Ok(()) }
}