use crate::{ProtocolError, Result};
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Decodable, Encodable};
use bytes::{BytesMut, Buf, BufMut};
use std::io::Cursor;
use kafka_protocol::messages::api_versions_response::ApiVersion;

pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

pub struct ResponseHeader {
    pub correlation_id: i32,
}

impl RequestHeader {
    pub fn decode(buf: &mut BytesMut) -> Result<Self> {
        let api_key = buf.get_i16();
        let api_version = buf.get_i16();
        let correlation_id = buf.get_i32();
        let client_id_len = buf.get_i16();
        let client_id = if client_id_len >= 0 {
            let mut client_id_bytes = vec![0u8; client_id_len as usize];
            buf.copy_to_slice(&mut client_id_bytes);
            Some(String::from_utf8(client_id_bytes).map_err(|e| ProtocolError::Decode(e.to_string()))?)
        } else {
            None
        };
        
        if api_version >= 1 {
            let _tagged_fields_count = buf.get_u8();
        }
        
        Ok(Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }
}

impl ResponseHeader {
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_i32(self.correlation_id);
        buf.put_u8(0);
    }
}

pub fn decode_request_header(buf: &mut BytesMut) -> Result<RequestHeader> {
    RequestHeader::decode(buf)
}

pub fn encode_response_header(buf: &mut BytesMut, correlation_id: i32) {
    ResponseHeader { correlation_id }.encode(buf);
}

macro_rules! handle_request {
    ($buf:expr, $version:expr, $req_type:ty) => {{
        let mut cursor = Cursor::new($buf.as_ref());
        <$req_type>::decode(&mut cursor, $version)
            .map_err(|e| ProtocolError::Decode(e.to_string()))
    }};
}

macro_rules! encode_response {
    ($resp:expr, $version:expr) => {{
        let mut buf = BytesMut::new();
        $resp.encode(&mut buf, $version)
            .map_err(|e| ProtocolError::Encode(e.to_string()))?;
        Ok(buf)
    }};
}

pub fn handle_api_versions_request(buf: &[u8], version: i16) -> Result<ApiVersionsResponse> {
    let mut cursor = Cursor::new(buf);
    let request = ApiVersionsRequest::decode(&mut cursor, version)
        .map_err(|e| ProtocolError::Decode(e.to_string()))?;
    
    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;
    
    let api_versions = vec![
        (0, 0, 9),   // Produce
        (1, 0, 13),  // Fetch
        (2, 0, 7),   // ListOffsets
        (3, 0, 12),  // Metadata
        (10, 0, 5),  // FindCoordinator
        (18, 0, 4),  // ApiVersions
        (22, 0, 4),  // InitProducerId
    ];
    
    for (api_key, min_v, max_v) in api_versions {
        response.api_keys.push(ApiVersion::default()
            .with_api_key(api_key)
            .with_min_version(min_v)
            .with_max_version(max_v));
    }
    
    if version >= 3 {
        response.throttle_time_ms = 0;
    }
    
    Ok(response)
}

pub fn encode_api_versions_response(response: &ApiVersionsResponse, version: i16) -> Result<BytesMut> {
    encode_response!(response, version)
}

pub fn handle_metadata_request(buf: &[u8], version: i16) -> Result<MetadataRequest> {
    let mut cursor = Cursor::new(buf);
    MetadataRequest::decode(&mut cursor, version)
        .map_err(|e| ProtocolError::Decode(e.to_string()))
}

pub fn encode_metadata_response(response: &MetadataResponse, version: i16) -> Result<BytesMut> {
    encode_response!(response, version)
}

pub fn handle_find_coordinator_request(buf: &[u8], version: i16) -> Result<FindCoordinatorRequest> {
    let mut cursor = Cursor::new(buf);
    FindCoordinatorRequest::decode(&mut cursor, version)
        .map_err(|e| ProtocolError::Decode(e.to_string()))
}

pub fn encode_find_coordinator_response(response: &FindCoordinatorResponse, version: i16) -> Result<BytesMut> {
    encode_response!(response, version)
}

pub fn handle_produce_request(buf: &[u8], version: i16) -> Result<ProduceRequest> {
    let mut cursor = Cursor::new(buf);
    ProduceRequest::decode(&mut cursor, version)
        .map_err(|e| ProtocolError::Decode(e.to_string()))
}

pub fn encode_produce_response(response: &ProduceResponse, version: i16) -> Result<BytesMut> {
    encode_response!(response, version)
}

pub fn handle_fetch_request(buf: &[u8], version: i16) -> Result<FetchRequest> {
    let mut cursor = Cursor::new(buf);
    FetchRequest::decode(&mut cursor, version)
        .map_err(|e| ProtocolError::Decode(e.to_string()))
}

pub fn encode_fetch_response(response: &FetchResponse, version: i16) -> Result<BytesMut> {
    encode_response!(response, version)
}

pub fn handle_init_producer_id_request(buf: &[u8], version: i16) -> Result<InitProducerIdRequest> {
    let mut cursor = Cursor::new(buf);
    InitProducerIdRequest::decode(&mut cursor, version)
        .map_err(|e| ProtocolError::Decode(e.to_string()))
}

pub fn encode_init_producer_id_response(response: &InitProducerIdResponse, version: i16) -> Result<BytesMut> {
    encode_response!(response, version)
}

pub fn handle_list_offsets_request(buf: &[u8], version: i16) -> Result<ListOffsetsRequest> {
    let mut cursor = Cursor::new(buf);
    ListOffsetsRequest::decode(&mut cursor, version)
        .map_err(|e| ProtocolError::Decode(e.to_string()))
}

pub fn encode_list_offsets_response(response: &ListOffsetsResponse, version: i16) -> Result<BytesMut> {
    encode_response!(response, version)
}

pub fn handle_offset_commit_request(buf: &[u8], version: i16) -> Result<OffsetCommitRequest> {
    let mut cursor = Cursor::new(buf);
    OffsetCommitRequest::decode(&mut cursor, version)
        .map_err(|e| ProtocolError::Decode(e.to_string()))
}

pub fn encode_offset_commit_response(response: &OffsetCommitResponse, version: i16) -> Result<BytesMut> {
    encode_response!(response, version)
}

pub fn handle_offset_fetch_request(buf: &[u8], version: i16) -> Result<OffsetFetchRequest> {
    let mut cursor = Cursor::new(buf);
    OffsetFetchRequest::decode(&mut cursor, version)
        .map_err(|e| ProtocolError::Decode(e.to_string()))
}

pub fn encode_offset_fetch_response(response: &OffsetFetchResponse, version: i16) -> Result<BytesMut> {
    encode_response!(response, version)
}

pub fn handle_join_group_request(buf: &[u8], version: i16) -> Result<JoinGroupRequest> {
    let mut cursor = Cursor::new(buf);
    JoinGroupRequest::decode(&mut cursor, version)
        .map_err(|e| ProtocolError::Decode(e.to_string()))
}

pub fn encode_join_group_response(response: &JoinGroupResponse, version: i16) -> Result<BytesMut> {
    encode_response!(response, version)
}

pub fn handle_heartbeat_request(buf: &[u8], version: i16) -> Result<HeartbeatRequest> {
    let mut cursor = Cursor::new(buf);
    HeartbeatRequest::decode(&mut cursor, version)
        .map_err(|e| ProtocolError::Decode(e.to_string()))
}

pub fn encode_heartbeat_response(response: &HeartbeatResponse, version: i16) -> Result<BytesMut> {
    encode_response!(response, version)
}

pub fn handle_leave_group_request(buf: &[u8], version: i16) -> Result<LeaveGroupRequest> {
    let mut cursor = Cursor::new(buf);
    LeaveGroupRequest::decode(&mut cursor, version)
        .map_err(|e| ProtocolError::Decode(e.to_string()))
}

pub fn encode_leave_group_response(response: &LeaveGroupResponse, version: i16) -> Result<BytesMut> {
    encode_response!(response, version)
}

pub fn handle_sync_group_request(buf: &[u8], version: i16) -> Result<SyncGroupRequest> {
    let mut cursor = Cursor::new(buf);
    SyncGroupRequest::decode(&mut cursor, version)
        .map_err(|e| ProtocolError::Decode(e.to_string()))
}

pub fn encode_sync_group_response(response: &SyncGroupResponse, version: i16) -> Result<BytesMut> {
    encode_response!(response, version)
}

pub fn handle_describe_groups_request(buf: &[u8], version: i16) -> Result<DescribeGroupsRequest> {
    let mut cursor = Cursor::new(buf);
    DescribeGroupsRequest::decode(&mut cursor, version)
        .map_err(|e| ProtocolError::Decode(e.to_string()))
}

pub fn encode_describe_groups_response(response: &DescribeGroupsResponse, version: i16) -> Result<BytesMut> {
    encode_response!(response, version)
}

pub fn handle_sasl_handshake_request(buf: &[u8], version: i16) -> Result<SaslHandshakeRequest> {
    let mut cursor = Cursor::new(buf);
    SaslHandshakeRequest::decode(&mut cursor, version)
        .map_err(|e| ProtocolError::Decode(e.to_string()))
}

pub fn encode_sasl_handshake_response(response: &SaslHandshakeResponse, version: i16) -> Result<BytesMut> {
    encode_response!(response, version)
}

pub fn handle_sasl_authenticate_request(buf: &[u8], version: i16) -> Result<SaslAuthenticateRequest> {
    let mut cursor = Cursor::new(buf);
    SaslAuthenticateRequest::decode(&mut cursor, version)
        .map_err(|e| ProtocolError::Decode(e.to_string()))
}

pub fn encode_sasl_authenticate_response(response: &SaslAuthenticateResponse, version: i16) -> Result<BytesMut> {
    encode_response!(response, version)
}

pub fn create_error_response(api_key: i16, api_version: i16, correlation_id: i32, error_code: i16) -> Result<BytesMut> {
    let mut buf = BytesMut::new();
    encode_response_header(&mut buf, correlation_id);
    
    match api_key {
        18 => {
            let mut response = ApiVersionsResponse::default();
            response.error_code = error_code;
            if api_version >= 3 {
                response.throttle_time_ms = 0;
            }
            response.encode(&mut buf, api_version)
                .map_err(|e| ProtocolError::Encode(e.to_string()))?;
        }
        3 => {
            let mut response = MetadataResponse::default();
            if api_version >= 1 {
                response.throttle_time_ms = 0;
            }
            response.error_code = error_code;
            response.encode(&mut buf, api_version)
                .map_err(|e| ProtocolError::Encode(e.to_string()))?;
        }
        10 => {
            let mut response = FindCoordinatorResponse::default();
            response.error_code = error_code;
            if api_version >= 1 {
                response.throttle_time_ms = 0;
            }
            response.encode(&mut buf, api_version)
                .map_err(|e| ProtocolError::Encode(e.to_string()))?;
        }
        0 => {
            let mut response = ProduceResponse::default();
            response.throttle_time_ms = 0;
            response.encode(&mut buf, api_version)
                .map_err(|e| ProtocolError::Encode(e.to_string()))?;
        }
        1 => {
            let mut response = FetchResponse::default();
            response.throttle_time_ms = 0;
            if api_version >= 7 {
                response.error_code = error_code;
                response.session_id = 0;
            }
            response.encode(&mut buf, api_version)
                .map_err(|e| ProtocolError::Encode(e.to_string()))?;
        }
        22 => {
            let mut response = InitProducerIdResponse::default();
            response.error_code = error_code;
            if api_version >= 1 {
                response.throttle_time_ms = 0;
            }
            response.encode(&mut buf, api_version)
                .map_err(|e| ProtocolError::Encode(e.to_string()))?;
        }
        _ => {
            return Err(ProtocolError::UnsupportedVersion(api_key));
        }
    }
    
    Ok(buf)
}