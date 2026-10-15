//! Produce request handling with filters, event IDs, breaker, and channels

use crate::{ProxyCoreError, Result};
use kp_protocol::{handler::*, ProtocolError};
use kp_metadata::MetadataRewriter;
use kp_filters::{FilterChain, FilterBatch, FilterRecord};
use kp_eventid::EventIdGenerator;
use kp_breaker::BreakerManager;
use kp_channel::{ChannelBatch, ChannelRecord, AckPolicyEngine};
use kafka_protocol::messages::*;
use kafka_protocol::messages::produce_response::{TopicProduceResponse, PartitionProduceResponse};
use kafka_protocol::protocol::StrBytes;
use std::sync::Arc;
use bytes::{BytesMut};
use tracing::{debug, error, warn};

pub struct ProduceHandler {
    filter_chain: Arc<FilterChain>,
    event_id_generator: Arc<EventIdGenerator>,
    breaker_manager: Arc<BreakerManager>,
    ack_policy_engine: Option<AckPolicyEngine>,
    metadata_rewriter: Arc<MetadataRewriter>,
}

impl ProduceHandler {
    pub fn new(
        filter_chain: Arc<FilterChain>,
        event_id_generator: Arc<EventIdGenerator>,
        breaker_manager: Arc<BreakerManager>,
        ack_policy_engine: Option<AckPolicyEngine>,
        metadata_rewriter: Arc<MetadataRewriter>,
    ) -> Self {
        Self {
            filter_chain,
            event_id_generator,
            breaker_manager,
            ack_policy_engine,
            metadata_rewriter,
        }
    }

    pub async fn handle_produce(
        &self,
        api_version: i16,
        correlation_id: i32,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        // Parse the produce request
        let _request = handle_produce_request(payload, api_version)
            .map_err(|e| ProxyCoreError::Protocol(e))?;

        // Convert to filter batch (simplified for now)
        let mut filter_batch = FilterBatch {
            topic: "default".to_string(),
            partition: 0,
            records: vec![],
            event_id_mode: kp_eventid::EventIdMode::Preserve,
            per_record: false,
            producer_id: None,
            producer_epoch: None,
            base_sequence: None,
        };

        // Check breaker before processing
        if self.breaker_manager.is_open("primary", "primary") {
            warn!("Breaker is open, rejecting produce request");
            return self.create_error_response(api_version, correlation_id).await;
        }

        // Run filter chain
        filter_batch = self.filter_chain.process_batch(filter_batch)?;

        // Pre-send hooks
        self.filter_chain.pre_send(&mut filter_batch)?;

        // Send through channels using ack policy
        let send_results = if let Some(engine) = &self.ack_policy_engine {
            // Convert filter batch to channel batch (clone to preserve for post_send)
            let channel_batch = self.convert_to_channel_batch(filter_batch.clone())?;
            engine.send(channel_batch).await
                .map_err(|e| ProxyCoreError::Channel(e))?
        } else {
            vec![]
        };

        // Check results
        let success = send_results.iter().all(|r| r.success);

        // Post-send hooks
        let send_result = kp_filters::hooks::SendResult {
            success,
            error: if !success { send_results.iter().find_map(|r| r.error.clone()) } else { None },
            offset: send_results.first().and_then(|r| r.offset),
        };
        self.filter_chain.post_send(&filter_batch, &send_result)?;

        // Record breaker result
        let breaker = self.breaker_manager.get_local_breaker("primary");
        let _ = breaker.call(|| async {
            if success {
                Ok(())
            } else {
                Err(kp_breaker::BreakerError::Local("Send failed".to_string()))
            }
        }).await;

        // Build response - create a minimal valid ProduceResponse
        let response = self.create_produce_response(success);

        let response_payload = encode_produce_response(&response, api_version)
            .map_err(|e| ProxyCoreError::Protocol(e))?;

        let mut final_buf = BytesMut::new();
        encode_response_header(&mut final_buf, correlation_id);
        final_buf.extend_from_slice(&response_payload);

        Ok(final_buf.freeze().to_vec())
    }

    fn convert_to_channel_batch(&self, filter_batch: FilterBatch) -> Result<ChannelBatch> {
        let records = filter_batch.records.into_iter().map(|r| ChannelRecord {
            key: r.key,
            value: r.value,
            headers: r.headers,
            timestamp: r.timestamp,
            event_id: r.event_id,
        }).collect();

        Ok(ChannelBatch {
            topic: filter_batch.topic,
            partition: filter_batch.partition,
            records,
        })
    }

    fn create_produce_response(&self, success: bool) -> ProduceResponse {
        let error_code = if success { 0 } else { 7 }; // 7 = NOT_ENOUGH_REPLICAS
        
        let mut response = ProduceResponse::default();
        response.throttle_time_ms = 0;
        
        // Add a default topic response with partition response
        let mut topic_response = TopicProduceResponse::default();
        topic_response.name = TopicName(StrBytes::from("default"));
        
        let mut partition_response = PartitionProduceResponse::default();
        partition_response.index = 0;
        partition_response.error_code = error_code;
        partition_response.base_offset = if success { 0 } else { -1 };
        partition_response.log_append_time_ms = -1;
        
        topic_response.partition_responses.push(partition_response);
        response.responses.push(topic_response);
        
        response
    }

    async fn create_error_response(
        &self,
        api_version: i16,
        correlation_id: i32,
    ) -> Result<Vec<u8>> {
        let response = self.create_produce_response(false);

        let response_payload = encode_produce_response(&response, api_version)
            .map_err(|e| ProxyCoreError::Protocol(e))?;

        let mut final_buf = BytesMut::new();
        encode_response_header(&mut final_buf, correlation_id);
        final_buf.extend_from_slice(&response_payload);

        Ok(final_buf.freeze().to_vec())
    }
}