//! API version negotiation

use crate::{ProtocolError, Result};
use std::collections::HashMap;

pub struct ApiVersionRegistry {
    versions: HashMap<i16, (i16, i16)>, // api_key -> (min_version, max_version)
}

impl ApiVersionRegistry {
    pub fn new() -> Self {
        let mut versions = HashMap::new();
        // Produce
        versions.insert(0, (0, 9));
        // Fetch
        versions.insert(1, (0, 13));
        // ListOffsets
        versions.insert(2, (0, 7));
        // Metadata
        versions.insert(3, (0, 12));
        // LeaderAndIsr
        versions.insert(4, (0, 5));
        // StopReplica
        versions.insert(5, (0, 3));
        // UpdateMetadata
        versions.insert(6, (0, 8));
        // ControlledShutdown
        versions.insert(7, (0, 3));
        // OffsetCommit
        versions.insert(8, (0, 8));
        // OffsetFetch
        versions.insert(9, (0, 9));
        // FindCoordinator
        versions.insert(10, (0, 5));
        // JoinGroup
        versions.insert(11, (0, 7));
        // Heartbeat
        versions.insert(12, (0, 4));
        // LeaveGroup
        versions.insert(13, (0, 4));
        // SyncGroup
        versions.insert(14, (0, 5));
        // DescribeGroups
        versions.insert(15, (0, 5));
        // ListGroups
        versions.insert(16, (0, 4));
        // SaslHandshake
        versions.insert(17, (0, 1));
        // ApiVersions
        versions.insert(18, (0, 4));
        // CreateTopics
        versions.insert(19, (0, 7));
        // DeleteTopics
        versions.insert(20, (0, 6));
        // DeleteRecords
        versions.insert(21, (0, 2));
        // InitProducerId
        versions.insert(22, (0, 4));
        // OffsetForLeaderEpoch
        versions.insert(23, (0, 4));
        // AddPartitionsToTxn
        versions.insert(24, (0, 3));
        // AddOffsetsToTxn
        versions.insert(25, (0, 3));
        // EndTxn
        versions.insert(26, (0, 3));
        // WriteTxnMarkers
        versions.insert(27, (0, 2));
        // TxnOffsetCommit
        versions.insert(28, (0, 3));
        // DescribeAcls
        versions.insert(29, (0, 2));
        // CreateAcls
        versions.insert(30, (0, 2));
        // DeleteAcls
        versions.insert(31, (0, 2));
        // DescribeConfigs
        versions.insert(32, (0, 4));
        // AlterConfigs
        versions.insert(33, (0, 2));
        // AlterReplicaLogDirs
        versions.insert(34, (0, 2));
        // DescribeLogDirs
        versions.insert(35, (0, 2));
        // SaslAuthenticate
        versions.insert(36, (0, 2));
        // CreatePartitions
        versions.insert(37, (0, 2));
        // CreateDelegationToken
        versions.insert(38, (0, 2));
        // RenewDelegationToken
        versions.insert(39, (0, 2));
        // ExpireDelegationToken
        versions.insert(40, (0, 2));
        // DescribeDelegationToken
        versions.insert(41, (0, 2));
        // DeleteGroups
        versions.insert(42, (0, 1));
        // ElectLeaders
        versions.insert(43, (0, 1));
        // IncrementalAlterConfigs
        versions.insert(44, (0, 1));
        // AlterPartitionReassignments
        versions.insert(45, (0, 1));
        // ListPartitionReassignments
        versions.insert(46, (0, 1));
        // OffsetDelete
        versions.insert(47, (0, 1));
        // DescribeClientQuotas
        versions.insert(48, (0, 2));
        // AlterClientQuotas
        versions.insert(49, (0, 1));
        // DescribeUserScramCredentials
        versions.insert(50, (0, 1));
        // AlterUserScramCredentials
        versions.insert(51, (0, 1));
        // Vote
        versions.insert(52, (0, 1));
        // BeginQuorumEpoch
        versions.insert(53, (0, 1));
        // EndQuorumEpoch
        versions.insert(54, (0, 1));
        // DescribeQuorum
        versions.insert(55, (0, 1));
        // AlterPartition
        versions.insert(56, (0, 1));
        // UpdateFeatures
        versions.insert(57, (0, 1));
        // DescribeCluster
        versions.insert(60, (0, 1));
        // DescribeProducers
        versions.insert(61, (0, 1));
        // BrokerRegistration
        versions.insert(62, (0, 1));
        // BrokerHeartbeat
        versions.insert(63, (0, 1));
        // UnregisterBroker
        versions.insert(64, (0, 1));
        // DescribeTransactions
        versions.insert(65, (0, 1));
        // ListTransactions
        versions.insert(66, (0, 1));
        // AllocateProducerIds
        versions.insert(67, (0, 1));
        // ConsumerGroupDescribe
        versions.insert(68, (0, 1));
        // ConsumerGroupHeartbeat
        versions.insert(69, (0, 1));
        // ShareFetch
        versions.insert(70, (0, 1));
        // ShareAcknowledge
        versions.insert(71, (0, 1));
        
        Self { versions }
    }

    pub fn get_versions(&self, api_key: i16) -> Option<(i16, i16)> {
        self.versions.get(&api_key).copied()
    }

    pub fn negotiate(&self, client_versions: &[(i16, i16, i16)]) -> Result<Vec<(i16, i16, i16)>> {
        let mut result = Vec::new();
        for (api_key, min_v, max_v) in client_versions {
            if let Some((server_min, server_max)) = self.get_versions(*api_key) {
                let negotiated_min = (*min_v).max(server_min);
                let negotiated_max = (*max_v).min(server_max);
                if negotiated_min <= negotiated_max {
                    result.push((*api_key, negotiated_min, negotiated_max));
                } else {
                    return Err(ProtocolError::NegotiationFailed(
                        format!("No compatible version for API {}: client [{}-{}], server [{}-{}]",
                            api_key, min_v, max_v, server_min, server_max)
                    ));
                }
            }
        }
        Ok(result)
    }
}