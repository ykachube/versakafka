//! Connection handling

use crate::{ProxyCoreError, Result, produce::ProduceHandler};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use bytes::{BytesMut};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, warn};
use futures_util::{SinkExt, StreamExt};
use kp_protocol::{
    frame::FRAME_HEADER_SIZE,
    handler::*,
    ProtocolError,
};
use kp_metadata::MetadataRewriter;
use kafka_protocol::messages::*;
use kp_filters::FilterChain;
use kp_eventid::EventIdGenerator;
use kp_breaker::BreakerManager;
use kp_channel::{AckPolicyEngine, Channel};

pub struct Connection {
    stream: Framed<TcpStream, crate::framing::KafkaFrameCodec>,
    config: Arc<super::config::ProxyConfig>,
    metadata_rewriter: Arc<MetadataRewriter>,
    backpressure: Arc<crate::backpressure::Backpressure>,
    produce_handler: Arc<ProduceHandler>,
}

impl Connection {
    pub fn new(
        stream: TcpStream, 
        config: Arc<super::config::ProxyConfig>,
        metadata_rewriter: Arc<MetadataRewriter>,
        backpressure: Arc<crate::backpressure::Backpressure>,
        filter_chain: Arc<FilterChain>,
        event_id_generator: Arc<EventIdGenerator>,
        breaker_manager: Arc<BreakerManager>,
        ack_policy_engine: Option<AckPolicyEngine>,
    ) -> Self {
        let produce_handler = Arc::new(ProduceHandler::new(
            filter_chain,
            event_id_generator,
            breaker_manager,
            ack_policy_engine,
            metadata_rewriter.clone(),
        ));
        
        Self {
            stream: Framed::new(stream, crate::framing::KafkaFrameCodec),
            config,
            metadata_rewriter,
            backpressure,
            produce_handler,
        }
    }

    pub async fn handle(&mut self) -> Result<()> {
        loop {
            match self.stream.next().await {
                Some(Ok(request_bytes)) => {
                    let start = Instant::now();
                    if let Err(e) = self.handle_request(request_bytes).await {
                        error!("Error handling request: {}", e);
                        return Err(e);
                    }
                    let latency = start.elapsed();
                    debug!("Request handled in {:?}", latency);
                }
                Some(Err(e)) => {
                    warn!("Frame decode error: {}", e);
                    return Err(e);
                }
                None => {
                    debug!("Connection closed by client");
                    return Ok(());
                }
            }
        }
    }

    async fn handle_request(&mut self, request_bytes: Vec<u8>) -> Result<()> {
        if request_bytes.len() < FRAME_HEADER_SIZE + 8 {
            return Err(ProxyCoreError::Framing("Request too small".to_string()));
        }

        let mut buf = BytesMut::from(&request_bytes[FRAME_HEADER_SIZE..]);
        
        let header = decode_request_header(&mut buf)
            .map_err(|e| ProxyCoreError::Framing(e.to_string()))?;

        debug!("Received request: api_key={}, api_version={}, correlation_id={}", 
            header.api_key, header.api_version, header.correlation_id);

        let response_bytes = self.process_request(header.api_key, header.api_version, header.correlation_id, &buf).await?;
        
        self.stream.send(response_bytes).await
            .map_err(|e| ProxyCoreError::Framing(e.to_string()))?;

        Ok(())
    }

    async fn process_request(
        &mut self,
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        let result = match api_key {
            18 => self.handle_api_versions(api_version, correlation_id, payload).await,
            3 => self.handle_metadata(api_version, correlation_id, payload).await,
            10 => self.handle_find_coordinator(api_version, correlation_id, payload).await,
            0 => self.handle_produce(api_version, correlation_id, payload).await,
            1 => self.handle_fetch(api_version, correlation_id, payload).await,
            22 => self.handle_init_producer_id(api_version, correlation_id, payload).await,
            2 => self.handle_list_offsets(api_version, correlation_id, payload).await,
            8 => self.handle_offset_commit(api_version, correlation_id, payload).await,
            9 => self.handle_offset_fetch(api_version, correlation_id, payload).await,
            11 => self.handle_join_group(api_version, correlation_id, payload).await,
            12 => self.handle_heartbeat(api_version, correlation_id, payload).await,
            13 => self.handle_leave_group(api_version, correlation_id, payload).await,
            14 => self.handle_sync_group(api_version, correlation_id, payload).await,
            15 => self.handle_describe_groups(api_version, correlation_id, payload).await,
            17 => self.handle_sasl_handshake(api_version, correlation_id, payload).await,
            36 => self.handle_sasl_authenticate(api_version, correlation_id, payload).await,
            _ => {
                warn!("Unsupported API key: {}", api_key);
                let error_bytes = create_error_response(api_key, api_version, correlation_id, 35)
                    .map_err(|e| ProxyCoreError::Protocol(e))?;
                Ok(error_bytes.freeze().to_vec())
            }
        };
        result
    }

    async fn handle_api_versions(
        &mut self,
        api_version: i16,
        correlation_id: i32,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        let response = handle_api_versions_request(payload, api_version)
            .map_err(|e| ProxyCoreError::Protocol(e))?;
        let response_payload = encode_api_versions_response(&response, api_version)
            .map_err(|e| ProxyCoreError::Protocol(e))?;
        
        let mut final_buf = BytesMut::new();
        encode_response_header(&mut final_buf, correlation_id);
        final_buf.extend_from_slice(&response_payload);
        
        Ok(final_buf.freeze().to_vec())
    }

    async fn handle_metadata(
        &mut self,
        api_version: i16,
        correlation_id: i32,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        let _request = handle_metadata_request(payload, api_version)
            .map_err(|e| ProxyCoreError::Protocol(e))?;
        
        let mut response = MetadataResponse::default();
        if api_version >= 1 {
            response.throttle_time_ms = 0;
        }
        response.error_code = 0;
        response.cluster_id = Some("kp-proxy".into());
        response.controller_id = 1.into();
        
        use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
        response.brokers.push(MetadataResponseBroker::default()
            .with_node_id(1.into())
            .with_host("localhost".into())
            .with_port(9092)
            .with_rack(None));
        
        self.metadata_rewriter.rewrite_metadata_response(&mut response)
            .map_err(|e| ProxyCoreError::Metadata(e))?;
        
        let response_payload = encode_metadata_response(&response, api_version)
            .map_err(|e| ProxyCoreError::Protocol(e))?;
        
        let mut final_buf = BytesMut::new();
        encode_response_header(&mut final_buf, correlation_id);
        final_buf.extend_from_slice(&response_payload);
        
        Ok(final_buf.freeze().to_vec())
    }

async fn handle_find_coordinator(
            &mut self,
            api_version: i16,
            correlation_id: i32,
            payload: &[u8],
        ) -> Result<Vec<u8>> {
            let _request = handle_find_coordinator_request(payload, api_version)
                .map_err(|e| ProxyCoreError::Protocol(e))?;
            
            let mut response = FindCoordinatorResponse::default();
            response.error_code = 0;
            if api_version >= 1 {
                response.throttle_time_ms = 0;
            }
            
            use kafka_protocol::messages::find_coordinator_response::Coordinator;
            response.coordinators.push(Coordinator::default()
                .with_error_code(0)
                .with_node_id(1.into())
                .with_host("localhost".into())
                .with_port(9092));
            
            self.metadata_rewriter.rewrite_find_coordinator_response(&mut response)
                .map_err(|e| ProxyCoreError::Metadata(e))?;
            
            let response_payload = encode_find_coordinator_response(&response, api_version)
                .map_err(|e| ProxyCoreError::Protocol(e))?;
            
            let mut final_buf = BytesMut::new();
            encode_response_header(&mut final_buf, correlation_id);
            final_buf.extend_from_slice(&response_payload);
            
            Ok(final_buf.freeze().to_vec())
        }

    async fn handle_produce(
        &mut self,
        api_version: i16,
        correlation_id: i32,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        self.produce_handler.handle_produce(api_version, correlation_id, payload).await
    }

    async fn handle_fetch(
        &mut self,
        api_version: i16,
        correlation_id: i32,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        let mut response = FetchResponse::default();
        response.throttle_time_ms = 0;
        if api_version >= 7 {
            response.error_code = 0;
            response.session_id = 0;
        }
        
        let response_payload = encode_fetch_response(&response, api_version)
            .map_err(|e| ProxyCoreError::Protocol(e))?;
        
        let mut final_buf = BytesMut::new();
        encode_response_header(&mut final_buf, correlation_id);
        final_buf.extend_from_slice(&response_payload);
        
        Ok(final_buf.freeze().to_vec())
    }

async fn handle_init_producer_id(
            &mut self,
            api_version: i16,
            correlation_id: i32,
            payload: &[u8],
        ) -> Result<Vec<u8>> {
            let _request = handle_init_producer_id_request(payload, api_version)
                .map_err(|e| ProxyCoreError::Protocol(e))?;
            
            let mut response = InitProducerIdResponse::default();
            response.error_code = 0;
            response.producer_id = kafka_protocol::messages::ProducerId(1);
            response.producer_epoch = 0;
            if api_version >= 1 {
                response.throttle_time_ms = 0;
            }
            
            let response_payload = encode_init_producer_id_response(&response, api_version)
                .map_err(|e| ProxyCoreError::Protocol(e))?;
            
            let mut final_buf = BytesMut::new();
            encode_response_header(&mut final_buf, correlation_id);
            final_buf.extend_from_slice(&response_payload);
            
            Ok(final_buf.freeze().to_vec())
        }

    async fn handle_list_offsets(
        &mut self,
        api_version: i16,
        correlation_id: i32,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        let mut response = ListOffsetsResponse::default();
        response.throttle_time_ms = 0;
        
        let response_payload = encode_list_offsets_response(&response, api_version)
            .map_err(|e| ProxyCoreError::Protocol(e))?;
        
        let mut final_buf = BytesMut::new();
        encode_response_header(&mut final_buf, correlation_id);
        final_buf.extend_from_slice(&response_payload);
        
        Ok(final_buf.freeze().to_vec())
    }

    async fn handle_offset_commit(
        &mut self,
        api_version: i16,
        correlation_id: i32,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        let mut response = OffsetCommitResponse::default();
        response.throttle_time_ms = 0;
        
        let response_payload = encode_offset_commit_response(&response, api_version)
            .map_err(|e| ProxyCoreError::Protocol(e))?;
        
        let mut final_buf = BytesMut::new();
        encode_response_header(&mut final_buf, correlation_id);
        final_buf.extend_from_slice(&response_payload);
        
        Ok(final_buf.freeze().to_vec())
    }

    async fn handle_offset_fetch(
        &mut self,
        api_version: i16,
        correlation_id: i32,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        let mut response = OffsetFetchResponse::default();
        response.throttle_time_ms = 0;
        
        let response_payload = encode_offset_fetch_response(&response, api_version)
            .map_err(|e| ProxyCoreError::Protocol(e))?;
        
        let mut final_buf = BytesMut::new();
        encode_response_header(&mut final_buf, correlation_id);
        final_buf.extend_from_slice(&response_payload);
        
        Ok(final_buf.freeze().to_vec())
    }

async fn handle_join_group(
            &mut self,
            api_version: i16,
            correlation_id: i32,
            payload: &[u8],
        ) -> Result<Vec<u8>> {
            let mut response = JoinGroupResponse::default();
            response.error_code = 0;
            response.generation_id = 0;
            response.protocol_type = Some("consumer".into());
            response.protocol_name = Some("range".into());
            response.leader = "".into();
            response.member_id = "".into();
            response.members.push(kafka_protocol::messages::join_group_response::JoinGroupResponseMember::default()
                .with_member_id("".into())
                .with_group_instance_id(None)
                .with_metadata(bytes::Bytes::new()));
            if api_version >= 4 {
                response.throttle_time_ms = 0;
            }
            
            let response_payload = encode_join_group_response(&response, api_version)
                .map_err(|e| ProxyCoreError::Protocol(e))?;
            
            let mut final_buf = BytesMut::new();
            encode_response_header(&mut final_buf, correlation_id);
            final_buf.extend_from_slice(&response_payload);
            
            Ok(final_buf.freeze().to_vec())
        }

    async fn handle_heartbeat(
        &mut self,
        api_version: i16,
        correlation_id: i32,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        let mut response = HeartbeatResponse::default();
        response.error_code = 0;
        if api_version >= 1 {
            response.throttle_time_ms = 0;
        }
        
        let response_payload = encode_heartbeat_response(&response, api_version)
            .map_err(|e| ProxyCoreError::Protocol(e))?;
        
        let mut final_buf = BytesMut::new();
        encode_response_header(&mut final_buf, correlation_id);
        final_buf.extend_from_slice(&response_payload);
        
        Ok(final_buf.freeze().to_vec())
    }

    async fn handle_leave_group(
        &mut self,
        api_version: i16,
        correlation_id: i32,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        let mut response = LeaveGroupResponse::default();
        response.error_code = 0;
        if api_version >= 1 {
            response.throttle_time_ms = 0;
        }
        
        let response_payload = encode_leave_group_response(&response, api_version)
            .map_err(|e| ProxyCoreError::Protocol(e))?;
        
        let mut final_buf = BytesMut::new();
        encode_response_header(&mut final_buf, correlation_id);
        final_buf.extend_from_slice(&response_payload);
        
        Ok(final_buf.freeze().to_vec())
    }

    async fn handle_sync_group(
        &mut self,
        api_version: i16,
        correlation_id: i32,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        let mut response = SyncGroupResponse::default();
        response.error_code = 0;
        if api_version >= 1 {
            response.throttle_time_ms = 0;
        }
        
        let response_payload = encode_sync_group_response(&response, api_version)
            .map_err(|e| ProxyCoreError::Protocol(e))?;
        
        let mut final_buf = BytesMut::new();
        encode_response_header(&mut final_buf, correlation_id);
        final_buf.extend_from_slice(&response_payload);
        
        Ok(final_buf.freeze().to_vec())
    }

    async fn handle_describe_groups(
        &mut self,
        api_version: i16,
        correlation_id: i32,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        let mut response = DescribeGroupsResponse::default();
        response.throttle_time_ms = 0;
        
        let response_payload = encode_describe_groups_response(&response, api_version)
            .map_err(|e| ProxyCoreError::Protocol(e))?;
        
        let mut final_buf = BytesMut::new();
        encode_response_header(&mut final_buf, correlation_id);
        final_buf.extend_from_slice(&response_payload);
        
        Ok(final_buf.freeze().to_vec())
    }

    async fn handle_sasl_handshake(
        &mut self,
        api_version: i16,
        correlation_id: i32,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        let mut response = SaslHandshakeResponse::default();
        response.error_code = 0;
        response.mechanisms.push("PLAIN".into());
        response.mechanisms.push("SCRAM-SHA-256".into());
        response.mechanisms.push("SCRAM-SHA-512".into());
        
        let response_payload = encode_sasl_handshake_response(&response, api_version)
            .map_err(|e| ProxyCoreError::Protocol(e))?;
        
        let mut final_buf = BytesMut::new();
        encode_response_header(&mut final_buf, correlation_id);
        final_buf.extend_from_slice(&response_payload);
        
        Ok(final_buf.freeze().to_vec())
    }

async fn handle_sasl_authenticate(
            &mut self,
            api_version: i16,
            correlation_id: i32,
            payload: &[u8],
        ) -> Result<Vec<u8>> {
            let mut response = SaslAuthenticateResponse::default();
            response.error_code = 0;
            // SaslAuthenticateResponse doesn't have throttle_time_ms in all versions
            
            let response_payload = encode_sasl_authenticate_response(&response, api_version)
                .map_err(|e| ProxyCoreError::Protocol(e))?;
            
            let mut final_buf = BytesMut::new();
            encode_response_header(&mut final_buf, correlation_id);
            final_buf.extend_from_slice(&response_payload);
            
            Ok(final_buf.freeze().to_vec())
        }
}