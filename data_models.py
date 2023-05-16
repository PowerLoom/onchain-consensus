import time
from enum import Enum
from typing import Dict
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
    submission_window: int
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


class AnchorRPCConfig(BaseModel):
    full_nodes: List[RPCNodeConfig]
    archive_nodes: Optional[List[RPCNodeConfig]]
    force_archive_blocks: Optional[int]
    retry: int
    chain_id: int
    protocol_state_address: str
    owner_address: str
    owner_private_key: str
    request_time_out: int


class EpochConfig(BaseModel):
    height: int
    head_offset: int
    block_time: int
    history_length: int


class ChainConfig(BaseModel):
    rpc: RPCConfig
    chain_id: int
    epoch: EpochConfig


class RLimit(BaseModel):
    file_descriptors: int


class SettingsConf(BaseModel):
    consensus_service: ConsensusService
    redis: RedisConfig
    chain: ChainConfig
    anchor_chain_rpc: AnchorRPCConfig
    rate_limit: str
    rlimit: RLimit
    ticker_begin_block: Optional[int]


class Epoch(BaseModel):
    sourcechainEndheight: int
    finalized: bool


class Message(BaseModel):
    message: str


class SnapshotterIssue(BaseModel):
    instanceID: str
    issueType: str
    projectID: str
    epochId: int
    timeOfReporting: int
    extra: Optional[Dict] = dict()


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
