from typing import List

from pydantic import BaseModel


class RPCNodesObject(BaseModel):
    NODES: List[str]
    RETRY_LIMIT: int
