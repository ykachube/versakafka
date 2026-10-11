//! Fake Kafka broker for testing

use crate::TestKitError;
use kafka_protocol::messages::{
    ProduceRequest, ProduceResponse, MetadataRequest, MetadataResponse, 
    ApiVersionsRequest, ApiVersionsResponse, FetchRequest, FetchResponse,
    produce_response::{TopicProduceResponse, PartitionProduceResponse},
    metadata_response::{MetadataResponseBroker, MetadataResponseTopic, MetadataResponsePartition},
    fetch_response::{FetchableTopicResponse, PartitionData},
    api_versions_response::ApiVersion,
    TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable};
use kafka_protocol::records::{Record, RecordBatchEncoder, RecordBatchDecoder, RecordEncodeOptions, Compression, TimestampType};
use bytes::{BytesMut, Bytes};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use tracing::info;
use uuid::Uuid;
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;

pub struct FakeBroker {
    config: super::config::FakeBrokerConfig,
    state: Arc<RwLock<BrokerState>>,
}

#[derive(Default)]
struct BrokerState {
    topics: HashMap<String, TopicState>,
    delay_ms: u64,
    error_rate: f64,
}

#[derive(Default)]
struct TopicState {
    partitions: HashMap<i32, PartitionState>,
}

#[derive(Default)]
struct PartitionState {
    messages: Vec<StoredRecord>,
    hwm: i64,
}

#[derive(Debug, Clone)]
struct StoredRecord {
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<(String, Option<Vec<u8>>)>,
    timestamp: i64,
    offset: i64,
}

impl FakeBroker {
    pub fn new(config: super::config::FakeBrokerConfig) -> Self {
        let broker = Self {
            config: config.clone(),
            state: Arc::new(RwLock::new(BrokerState::default())),
        };
        {
            let mut state = broker.state.write();
            state.delay_ms = config.delay_ms;
            state.error_rate = config.error_rate;
        }
        broker
    }

    pub async fn start(&self) -> Result<(), TestKitError> {
        info!("Fake broker started");
        Ok(())
    }

    pub async fn stop(&self) -> Result<(), TestKitError> {
        info!("Fake broker stopped");
        Ok(())
    }

    /// Handle a Kafka request and return the response bytes
    pub fn handle_request(&self, api_key: i16, api_version: i16, request_bytes: &[u8]) -> Result<Vec<u8>, TestKitError> {
        // Simulate delay
        if self.config.delay_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(self.config.delay_ms));
        }

        // Simulate errors
        if self.config.error_rate > 0.0 && rand::random::<f64>() < self.config.error_rate {
            return Err(TestKitError::FakeBroker("Simulated error".to_string()));
        }

        match api_key {
            0 => self.handle_produce(api_version, request_bytes),
            3 => self.handle_metadata(api_version, request_bytes),
            18 => self.handle_api_versions(api_version, request_bytes),
            1 => self.handle_fetch(api_version, request_bytes),
            _ => Err(TestKitError::FakeBroker(format!("Unsupported API key: {}", api_key))),
        }
    }

    fn handle_produce(&self, api_version: i16, request_bytes: &[u8]) -> Result<Vec<u8>, TestKitError> {
        let mut buf = BytesMut::from(request_bytes);
        let request = ProduceRequest::decode(&mut buf, api_version)
            .map_err(|e| TestKitError::FakeBroker(format!("Produce decode error: {}", e)))?;
        
        let mut response = ProduceResponse::default();
        
        for topic_data in request.topic_data {
            let topic_name = topic_data.name.to_string();
            
            let mut partition_responses = Vec::new();
            for partition_data in topic_data.partition_data {
                let partition_id = partition_data.index;
                
                // Store messages - decode the record batch
                if let Some(records_bytes) = partition_data.records {
                    let mut records_buf = BytesMut::from(&records_bytes[..]);
                    // Decode record batches
                    let records = self.decode_record_batches(&mut records_buf)?;
                    
                    let mut state = self.state.write();
                    let topic_state = state.topics.entry(topic_name.clone()).or_default();
                    let partition_state = topic_state.partitions.entry(partition_id).or_default();
                    
                    for record in records {
                        let offset = partition_state.hwm;
                        partition_state.hwm += 1;
                        
                        let mut headers = Vec::new();
                        for (k, v) in record.headers {
                            headers.push((k.to_string(), v.map(|b| b.to_vec())));
                        }
                        
                        let stored_record = StoredRecord {
                            key: record.key.map(|k| k.to_vec()),
                            value: record.value.map(|v| v.to_vec()),
                            headers,
                            timestamp: record.timestamp,
                            offset,
                        };
                        partition_state.messages.push(stored_record);
                    }
                }
                
                let state = self.state.read();
                let hwm = state.topics.get(&topic_name)
                    .and_then(|t| t.partitions.get(&partition_id))
                    .map(|p| p.hwm)
                    .unwrap_or(0);
                
                partition_responses.push(PartitionProduceResponse::default()
                    .with_index(partition_id)
                    .with_error_code(0)
                    .with_base_offset(hwm - 1)
                    .with_log_append_time_ms(-1)
                    .with_log_start_offset(0));
            }
            
            let topic_name_str = topic_name.clone();
            response.responses.push(TopicProduceResponse::default()
                .with_name(TopicName(StrBytes::from_string(topic_name_str)))
                .with_partition_responses(partition_responses));
        }
        
        response.throttle_time_ms = 0;
        
        let mut buf = BytesMut::new();
        response.encode(&mut buf, api_version)
            .map_err(|e| TestKitError::FakeBroker(format!("Produce encode error: {}", e)))?;
        Ok(buf.to_vec())
    }

    fn decode_record_batches(&self, buf: &mut BytesMut) -> Result<Vec<Record>, TestKitError> {
        let record_sets = RecordBatchDecoder::decode_all(buf)
            .map_err(|e| TestKitError::FakeBroker(format!("Record batch decode error: {}", e)))?;
        
        let mut all_records = Vec::new();
        for record_set in record_sets {
            all_records.extend(record_set.records);
        }
        
        Ok(all_records)
    }

    fn handle_metadata(&self, api_version: i16, request_bytes: &[u8]) -> Result<Vec<u8>, TestKitError> {
        let mut buf = BytesMut::from(request_bytes);
        let request = MetadataRequest::decode(&mut buf, api_version)
            .map_err(|e| TestKitError::FakeBroker(format!("Metadata decode error: {}", e)))?;
        
        let mut response = MetadataResponse::default();
        
        let topics_to_return: Vec<String> = match &request.topics {
            Some(topics) if !topics.is_empty() => {
                topics.iter().filter_map(|t| t.name.as_ref().map(|n| n.to_string())).collect()
            }
            _ => {
                // Return all topics
                let state = self.state.read();
                state.topics.keys().cloned().collect()
            }
        };
        
        for topic_name in topics_to_return {
            let state = self.state.read();
            let topic_state = state.topics.get::<String>(&topic_name);
            
            let mut partitions = Vec::new();
            if let Some(topic_state) = topic_state {
                for (partition_id, _partition_state) in &topic_state.partitions {
                    partitions.push(MetadataResponsePartition::default()
                        .with_error_code(0)
                        .with_partition_index(*partition_id)
                        .with_leader_id(1.into())
                        .with_leader_epoch(0)
                        .with_replica_nodes(vec![1.into()])
                        .with_isr_nodes(vec![1.into()])
                        .with_offline_replicas(vec![]));
                }
            } else {
                // Topic doesn't exist
                partitions.push(MetadataResponsePartition::default()
                    .with_error_code(3) // UnknownTopicOrPartition
                    .with_partition_index(0)
                    .with_leader_id((-1).into())
                    .with_leader_epoch(-1)
                    .with_replica_nodes(vec![])
                    .with_isr_nodes(vec![])
                    .with_offline_replicas(vec![]));
            }
            
            response.topics.push(MetadataResponseTopic::default()
                .with_error_code(if topic_state.is_some() { 0 } else { 3 })
                .with_name(Some(TopicName(StrBytes::from_string(topic_name.clone()))))
                .with_topic_id(Uuid::nil())
                .with_is_internal(false)
                .with_partitions(partitions)
                .with_topic_authorized_operations(0));
        }
        
        // Add cluster info
        response.brokers.push(MetadataResponseBroker::default()
            .with_node_id(1.into())
            .with_host("localhost".into())
            .with_port(9092)
            .with_rack(None));
        response.cluster_id = Some("test-cluster".into());
        response.controller_id = 1.into();
        if api_version >= 3 {
            response.throttle_time_ms = 0;
        }
        
        let mut buf = BytesMut::new();
        response.encode(&mut buf, api_version)
            .map_err(|e| TestKitError::FakeBroker(format!("Metadata encode error: {}", e)))?;
        Ok(buf.to_vec())
    }

    fn handle_api_versions(&self, api_version: i16, request_bytes: &[u8]) -> Result<Vec<u8>, TestKitError> {
        let mut buf = BytesMut::from(request_bytes);
        let _request = ApiVersionsRequest::decode(&mut buf, api_version)
            .map_err(|e| TestKitError::FakeBroker(format!("ApiVersions decode error: {}", e)))?;
        
        let mut response = ApiVersionsResponse::default();
        response.error_code = 0;
        
        // Add supported API versions
        let api_versions = vec![
            (0, 0, 9),   // Produce
            (1, 0, 13),  // Fetch
            (2, 0, 7),   // ListOffsets
            (3, 0, 12),  // Metadata
            (18, 0, 4),  // ApiVersions
            (22, 0, 4),  // InitProducerId
        ];
        
        for (api_key, min_v, max_v) in api_versions {
            response.api_keys.push(ApiVersion::default()
                .with_api_key(api_key)
                .with_min_version(min_v)
                .with_max_version(max_v));
        }
        
        response.throttle_time_ms = 0;
        
        let mut buf = BytesMut::new();
        response.encode(&mut buf, api_version)
            .map_err(|e| TestKitError::FakeBroker(format!("ApiVersions encode error: {}", e)))?;
        Ok(buf.to_vec())
    }

    fn handle_fetch(&self, api_version: i16, request_bytes: &[u8]) -> Result<Vec<u8>, TestKitError> {
        let mut buf = BytesMut::from(request_bytes);
        let request = FetchRequest::decode(&mut buf, api_version)
            .map_err(|e| TestKitError::FakeBroker(format!("Fetch decode error: {}", e)))?;
        
        let mut response = FetchResponse::default();
        response.throttle_time_ms = 0;
        if api_version >= 7 {
            response.error_code = 0;
            response.session_id = 0;
        }
        
        for topic_data in request.topics {
            let topic_name = topic_data.topic.to_string();
            let state = self.state.read();
            let topic_state = state.topics.get(&topic_name);
            
            let mut partition_responses = Vec::new();
            for partition_data in topic_data.partitions {
                let partition_id = partition_data.partition;
                
                let high_watermark = if let Some(topic_state) = topic_state {
                    if let Some(partition_state) = topic_state.partitions.get(&partition_id) {
                        partition_state.hwm
                    } else {
                        0
                    }
                } else {
                    0
                };
                
                // Build records bytes for this partition
                let records_bytes = if let Some(topic_state) = topic_state {
                    if let Some(partition_state) = topic_state.partitions.get(&partition_id) {
                        let fetch_offset = partition_data.fetch_offset as usize;
                        if fetch_offset < partition_state.messages.len() {
                            self.encode_records_to_bytes(&partition_state.messages[fetch_offset..])
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                
                partition_responses.push(PartitionData::default()
                    .with_partition_index(partition_id)
                    .with_error_code(0)
                    .with_high_watermark(high_watermark)
                    .with_last_stable_offset(high_watermark)
                    .with_log_start_offset(0)
                    .with_aborted_transactions(None)
                    .with_preferred_read_replica((-1).into())
                    .with_records(records_bytes));
            }
            
            let topic_name_str = topic_name.clone();
            response.responses.push(FetchableTopicResponse::default()
                .with_topic(TopicName(StrBytes::from_string(topic_name_str)))
                .with_topic_id(Uuid::nil())
                .with_partitions(partition_responses));
        }
        
        let mut buf = BytesMut::new();
        response.encode(&mut buf, api_version)
            .map_err(|e| TestKitError::FakeBroker(format!("Fetch encode error: {}", e)))?;
        Ok(buf.to_vec())
    }

    fn encode_records_to_bytes(&self, records: &[StoredRecord]) -> Option<Bytes> {
        if records.is_empty() {
            return None;
        }
        
        let mut buf = BytesMut::new();
        let record_refs: Vec<Record> = records.iter().map(|r| {
            let mut headers = IndexMap::new();
            for (k, v) in &r.headers {
                let key = k.clone(); // Own the string
                headers.insert(StrBytes::from(key), v.as_ref().map(|b| Bytes::from(b.clone())));
            }
            
            Record {
                transactional: false,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: 0,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: TimestampType::Creation,
                offset: r.offset,
                sequence: -1,
                timestamp: r.timestamp,
                key: r.key.as_ref().map(|k| Bytes::from(k.clone())),
                value: r.value.as_ref().map(|v| Bytes::from(v.clone())),
                headers,
            }
        }).collect();
        
        let options = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };
        
        RecordBatchEncoder::encode(&mut buf, &record_refs, &options).ok()?;
        Some(buf.freeze())
    }

    /// Create a topic with specified partitions
    pub fn create_topic(&self, topic: &str, num_partitions: i32) {
        let mut state = self.state.write();
        let topic_state = state.topics.entry(topic.to_string()).or_default();
        for i in 0..num_partitions {
            topic_state.partitions.entry(i).or_default();
        }
    }

    /// Get message count for a topic/partition
    pub fn get_message_count(&self, topic: &str, partition: i32) -> usize {
        let state = self.state.read();
        state.topics.get(topic)
            .and_then(|t| t.partitions.get(&partition))
            .map(|p| p.messages.len())
            .unwrap_or(0)
    }

    /// Get all messages for a topic/partition
    pub fn get_messages(&self, topic: &str, partition: i32) -> Vec<StoredRecord> {
        let state = self.state.read();
        state.topics.get(topic)
            .and_then(|t| t.partitions.get(&partition))
            .map(|p| p.messages.clone())
            .unwrap_or_default()
    }

    /// Set delay for all requests
    pub fn set_delay(&self, delay_ms: u64) {
        let mut state = self.state.write();
        state.delay_ms = delay_ms;
    }

    /// Set error rate (0.0 to 1.0)
    pub fn set_error_rate(&self, error_rate: f64) {
        let mut state = self.state.write();
        state.error_rate = error_rate.clamp(0.0, 1.0);
    }
}

impl StoredRecord {
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }
    pub fn headers(&self) -> &[(String, Option<Vec<u8>>)] {
        &self.headers
    }
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }
    pub fn offset(&self) -> i64 {
        self.offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FakeBrokerConfig;
    use kafka_protocol::messages::{ProduceRequest, produce_request::{TopicProduceData, PartitionProduceData}, TopicName};
    use kafka_protocol::protocol::StrBytes;

    #[tokio::test]
    async fn test_fake_broker_produce() {
        let config = FakeBrokerConfig::default();
        let broker = FakeBroker::new(config);
        broker.create_topic("test-topic", 3);
        
        // Test produce request
        let mut request = ProduceRequest::default();
        request.acks = 1;
        request.timeout_ms = 1000;
        
        // Add topic data with empty records
        request.topic_data.push(TopicProduceData::default()
            .with_name(TopicName(StrBytes::from("test-topic")))
            .with_partition_data(vec![PartitionProduceData::default()
                .with_index(0)
                .with_records(None)]));
        
        let mut buf = BytesMut::new();
        request.encode(&mut buf, 9).unwrap();
        
        let response_bytes = broker.handle_request(0, 9, &buf).unwrap();
        assert!(!response_bytes.is_empty());
    }
}