import time
from enum import Enum
from typing import List
from typing import Optional

from pydantic import BaseModel


class RedisConfig(BaseModel):
    host: str
    port: int
    db: int
    password: Optional[str]


class PeerUUIDIncludedRequests(BaseModel):
    instanceID: str


class PeerRegistrationRequest(PeerUUIDIncludedRequests):
    projectID: str


class EpochBase(BaseModel):
    begin: int
    end: int


class SnapshotBase(PeerUUIDIncludedRequests):
    epoch: EpochBase
    projectID: str


class ConsensusService(BaseModel):
    host: str
    port: str
    keepalive_secs: int
    keys_ttl: int = 86400
    gunicorn_workers: int = 20


class NodeConfig(BaseModel):
    url: str


class RPCConfig(BaseModel):
    nodes: List[NodeConfig]
    retry: int
    request_timeout: int


class RPCNodeConfig(BaseModel):
    url: str
    rate_limit: str


class ConnectionLimits(BaseModel):
    max_connections: int = 100
    max_keepalive_connections: int = 50
    keepalive_expiry: int = 300


class RPCConfigBase(BaseModel):
    full_nodes: List[RPCNodeConfig]
    archive_nodes: Optional[List[RPCNodeConfig]]
    force_archive_blocks: Optional[int]
    retry: int
    request_time_out: int
    connection_limits: ConnectionLimits


class EpochConfig(BaseModel):
    height: int
    head_offset: int
    block_time: int
    history_length: int


class ChainConfig(BaseModel):
    rpc: RPCConfigBase
    chain_id: int
    epoch: EpochConfig


class AnchorChainConfig(BaseModel):
    rpc: RPCConfigBase
    chain_id: int
    polling_interval: int


class RLimit(BaseModel):
    file_descriptors: int


class SettingsConf(BaseModel):
    consensus_service: ConsensusService
    redis: RedisConfig
    chain: ChainConfig
    anchor_chain: AnchorChainConfig
    rate_limit: str
    rlimit: RLimit
    ticker_begin_block: Optional[int]
    protocol_state_address: str
    validator_epoch_address: str
    validator_epoch_private_key: str
    force_consensus_address: str
    force_consensus_private_key: str


class Epoch(BaseModel):
    sourcechainEndheight: int
    finalized: bool


class Message(BaseModel):
    message: str


class SnapshotterIssue(BaseModel):
    instanceID: str
    issueType: str
    projectID: str
    epochId: str
    timeOfReporting: str
    extra: Optional[str] = ''


class UserStatusEnum(str, Enum):
    active = 'active'
    inactive = 'inactive'


class SnapshotterMetadata(BaseModel):
    rate_limit: str
    active: UserStatusEnum
    callsCount: int = 0
    throttledCount: int = 0
    next_reset_at: int = int(time.time()) + 86400
    name: str
    email: str
    alias: str
    uuid: Optional[str] = None
