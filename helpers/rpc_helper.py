import time
from functools import wraps
from itertools import repeat

import requests

from .message_models import RPCNodesObject
from settings.conf import settings
from utils.default_logger import logger

rpc_logger = logger.bind(module='PowerLoom|OnChainConsensus|RPCHelper')
REQUEST_TIMEOUT = settings.chain.rpc.request_time_out
RETRY_LIMIT = settings.chain.rpc.retry


def auto_retry(tries=3, exc=Exception, delay=5):
    def deco(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for _ in range(tries):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    time.sleep(delay)
                    continue
            raise exc

        return wrapper

    return deco


class BailException(RuntimeError):
    pass


class ConstructRPC:
    def __init__(self, network_id):
        self._network_id = network_id
        self._querystring = {'id': network_id, 'jsonrpc': '2.0'}

    def sync_post_json_rpc(self, procedure, rpc_nodes: RPCNodesObject, params=None):
        q_s = self.construct_one_time_rpc(procedure=procedure, params=params)
        rpc_urls = rpc_nodes.NODES
        retry = dict(zip(rpc_urls, repeat(0)))
        success = False
        while True:
            if all(val == rpc_nodes.RETRY_LIMIT for val in retry.values()):
                rpc_logger.error(
                    'Retry limit reached for all RPC endpoints. Following request {}: {}',
                    q_s, retry,
                )
                raise BailException
            for _url in rpc_urls:
                try:
                    retry[_url] += 1
                    r = requests.post(_url, json=q_s, timeout=REQUEST_TIMEOUT)
                    json_response = r.json()
                except (
                    requests.exceptions.Timeout,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.HTTPError,
                ):
                    success = False
                except Exception as e:
                    success = False
                else:
                    if procedure == 'eth_getBlockByNumber' and not json_response['result']:
                        continue
                    success = True
                    return json_response

    def rpc_eth_blocknumber(self, rpc_nodes: RPCNodesObject):
        rpc_response = self.sync_post_json_rpc(
            procedure='eth_blockNumber', rpc_nodes=rpc_nodes,
        )
        try:
            new_blocknumber = int(rpc_response['result'], 16)
        except Exception as e:
            raise BailException
        else:
            return new_blocknumber

    @auto_retry(tries=RETRY_LIMIT, exc=BailException)
    def rpc_eth_getblock_by_number(self, blocknum, rpc_nodes):
        rpc_response = self.sync_post_json_rpc(
            procedure='eth_getBlockByNumber', rpc_nodes=rpc_nodes,
            params=[hex(blocknum), False],
        )
        try:
            blockdetails = rpc_response['result']
        except Exception as e:
            raise
        else:
            return blockdetails

    def construct_one_time_rpc(self, procedure, params, default_block=None):
        self._querystring['method'] = procedure
        self._querystring['params'] = []
        if type(params) is list:
            self._querystring['params'].extend(params)
        elif params is not None:
            self._querystring['params'].append(params)
        if default_block is not None:
            self._querystring['params'].append(default_block)
        return self._querystring
