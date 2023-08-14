import asyncio
import json
import random
import threading
import time
from functools import wraps
from multiprocessing import Process
from signal import SIGINT
from signal import signal
from signal import SIGQUIT
from signal import SIGTERM

import uvloop
from httpx import AsyncClient
from httpx import AsyncHTTPTransport
from httpx import Limits
from httpx import Timeout
from redis import asyncio as aioredis
from setproctitle import setproctitle
from web3 import AsyncHTTPProvider
from web3 import AsyncWeb3

from data_models import GenericTxnIssue
from exceptions import GenericExitOnSignal
from helpers.message_models import RPCNodesObject
from helpers.redis_keys import get_epoch_generator_epoch_history
from helpers.redis_keys import get_epoch_generator_last_epoch
from helpers.rpc_helper import ConstructRPC
from settings.conf import settings
from utils.chunk_helper import chunks
from utils.default_logger import logger
from utils.notification_utils import send_failure_notifications
from utils.redis_conn import RedisPool
from utils.transaction_utils import write_transaction
from utils.transaction_utils import write_transaction_with_receipt
protocol_state_contract_address = settings.protocol_state_address
# load abi from json file and create contract object
with open('utils/static/abi.json', 'r') as f:
    abi = json.load(f)

w3 = AsyncWeb3(AsyncHTTPProvider(settings.anchor_chain.rpc.full_nodes[0].url))
protocol_state_contract = w3.eth.contract(
    address=settings.protocol_state_address, abi=abi,
)


def redis_cleanup(fn):
    @wraps(fn)
    async def wrapper(self, *args, **kwargs):
        try:
            await fn(self, *args, **kwargs)
        except (GenericExitOnSignal, KeyboardInterrupt):
            try:
                self._logger.debug('Waiting for pushing latest epoch to Redis')
                if self.last_sent_block:
                    await self._writer_redis_pool.set(get_epoch_generator_last_epoch(), self.last_sent_block)

                    self._logger.debug(
                        'Shutting down after sending out last epoch with end block height as {},'
                        ' starting blockHeight to be used during next restart is {}', self.last_sent_block, self.last_sent_block + 1,
                    )
            except Exception as E:
                self._logger.error('Error while saving last state: {}', E)
        except Exception as E:
            self._logger.error('Error while running process: {}', E)
        finally:
            self._logger.debug('Shutting down')
    return wrapper


class EpochGenerator:
    _aioredis_pool: RedisPool
    _reader_redis_pool: aioredis.Redis
    _writer_redis_pool: aioredis.Redis

    def __init__(self, name='PowerLoom|OnChainConsensus|EpochGenerator'):
        self.name = name
        setproctitle(self.name)
        self._logger = logger.bind(module=self.name)
        self._shutdown_initiated = False
        self.last_sent_block = 0
        self._end = None
        self._nonce = -1
        self._async_transport = None
        self._client = None

    async def setup(self):
        self._aioredis_pool = RedisPool(writer_redis_conf=settings.redis)
        self._nonce = await w3.eth.get_transaction_count(
            settings.validator_epoch_address,
        )
        await self._aioredis_pool.populate()
        self._reader_redis_pool = self._aioredis_pool.reader_redis_pool
        self._writer_redis_pool = self._aioredis_pool.writer_redis_pool
        self.redis_thread: threading.Thread
        await self._init_httpx_client()

    async def _init_httpx_client(self):
        if self._async_transport is not None:
            return
        self._async_transport = AsyncHTTPTransport(
            limits=Limits(
                max_connections=100,
                max_keepalive_connections=50,
                keepalive_expiry=None,
            ),
        )
        self._client = AsyncClient(
            timeout=Timeout(timeout=30.0),
            follow_redirects=False,
            transport=self._async_transport,
        )

    def _generic_exit_handler(self, signum, sigframe):
        if signum in [SIGINT, SIGTERM, SIGQUIT] and not self._shutdown_initiated:
            self._shutdown_initiated = True
            raise GenericExitOnSignal

    async def _fetch_epoch_from_contract(self) -> int:
        last_epoch_data = await protocol_state_contract.functions.currentEpoch().call()
        if last_epoch_data[1]:
            self._logger.debug(
                'Found last epoch block : {} in contract. Starting from checkpoint.', last_epoch_data[
                    1
                ],
            )
            begin_block_epoch = last_epoch_data[1] + 1
            return begin_block_epoch
        else:
            self._logger.debug(
                'No last epoch block found in contract. Starting from configured block in settings.',
            )
            return -1

    @redis_cleanup
    async def run(self):
        await self.setup()

        begin_block_epoch = settings.ticker_begin_block if settings.ticker_begin_block else 0
        for signame in [SIGINT, SIGTERM, SIGQUIT]:
            signal(signame, self._generic_exit_handler)

        last_contract_epoch = await self._fetch_epoch_from_contract()
        if last_contract_epoch != -1:
            begin_block_epoch = last_contract_epoch

        # waiting to release epoch chunks every half of block time
        sleep_secs_between_chunks = settings.chain.epoch.block_time // 2

        rpc_obj = ConstructRPC(network_id=settings.chain.chain_id)
        rpc_urls = []
        for node in settings.chain.rpc.full_nodes:
            self._logger.debug('node {}', node.url)
            rpc_urls.append(node.url)
        rpc_nodes_obj = RPCNodesObject(
            NODES=rpc_urls,
            RETRY_LIMIT=settings.chain.rpc.retry,
        )
        self._logger.debug('Starting {}', Process.name)
        while True:
            try:
                cur_block = rpc_obj.rpc_eth_blocknumber(
                    rpc_nodes=rpc_nodes_obj,
                )
            except Exception as ex:
                self._logger.error(
                    'Unable to fetch latest block number due to RPC failure {}. Retrying after {} seconds.',
                    ex,
                    settings.chain.epoch.block_time,
                )
                await asyncio.sleep(settings.chain.epoch.block_time)
                continue
            else:
                self._logger.debug('Got current head of chain: {}', cur_block)
                if not begin_block_epoch:
                    self._logger.debug('Begin of epoch not set')
                    begin_block_epoch = cur_block
                    self._logger.debug(
                        'Set begin of epoch to current head of chain: {}', cur_block,
                    )
                    self._logger.debug(
                        'Sleeping for: {} seconds', settings.chain.epoch.block_time,
                    )
                    await asyncio.sleep(settings.chain.epoch.block_time)
                else:
                    # self._logger.debug('Picked begin of epoch: {}', begin_block_epoch)
                    end_block_epoch = cur_block - settings.chain.epoch.head_offset
                    if not (end_block_epoch - begin_block_epoch + 1) >= settings.chain.epoch.height:
                        sleep_factor = settings.chain.epoch.height - \
                            ((end_block_epoch - begin_block_epoch) + 1)
                        self._logger.debug(
                            'Current head of source chain estimated at block {} after offsetting | '
                            '{} - {} does not satisfy configured epoch length. '
                            'Sleeping for {} seconds for {} blocks to accumulate....',
                            end_block_epoch, begin_block_epoch, end_block_epoch,
                            sleep_factor * settings.chain.epoch.block_time, sleep_factor,
                        )
                        await asyncio.sleep(
                            sleep_factor *
                            settings.chain.epoch.block_time,
                        )
                        continue
                    self._logger.debug(
                        'Chunking blocks between {} - {} with chunk size: {}', begin_block_epoch,
                        end_block_epoch, settings.chain.epoch.height,
                    )
                    for epoch in chunks(begin_block_epoch, end_block_epoch, settings.chain.epoch.height):
                        if epoch[1] - epoch[0] + 1 < settings.chain.epoch.height:
                            self._logger.debug(
                                'Skipping chunk of blocks {} - {} as minimum epoch size not satisfied | '
                                'Resetting chunking to begin from block {}',
                                epoch[0], epoch[1], epoch[0],
                            )
                            begin_block_epoch = epoch[0]
                            break
                        epoch_block = {'begin': epoch[0], 'end': epoch[1]}
                        self._logger.debug(
                            'Epoch of sufficient length found: {}', epoch_block,
                        )

                        try:
                            self._logger.info('Attempting to release epoch {}', epoch_block)
                            rand = random.random()
                            if rand < 0.1:
                                tx_hash, receipt = await write_transaction_with_receipt(
                                    w3,
                                    settings.validator_epoch_address,
                                    settings.validator_epoch_private_key,
                                    protocol_state_contract,
                                    'releaseEpoch',
                                    self._nonce,
                                    epoch_block['begin'],
                                    epoch_block['end'],
                                )

                                if receipt['status'] != 1:
                                    self._logger.error(
                                        'Unable to release epoch, txn failed! Got receipt: {}', receipt,
                                    )

                                    issue = GenericTxnIssue(
                                        accountAddress=settings.validator_epoch_address,
                                        epochBegin=epoch_block['begin'],
                                        issueType='EpochReleaseTxnFailed',
                                        extra=json.dumps(receipt),
                                    )

                                    await send_failure_notifications(client=self._client, message=issue)

                                    # sleep for 30 seconds to avoid nonce collision
                                    time.sleep(30)
                                    # reset nonce
                                    self._nonce = await w3.eth.get_transaction_count(
                                        settings.validator_epoch_address,
                                    )

                                    last_contract_epoch = await self._fetch_epoch_from_contract()
                                    if last_contract_epoch != -1:
                                        begin_block_epoch = last_contract_epoch
                                    continue

                            else:
                                tx_hash = await write_transaction(
                                    w3,
                                    settings.validator_epoch_address,
                                    settings.validator_epoch_private_key,
                                    protocol_state_contract,
                                    'releaseEpoch',
                                    self._nonce,
                                    epoch_block['begin'],
                                    epoch_block['end'],
                                )

                            self._nonce += 1
                            self._logger.debug(
                                'Epoch Released! Transaction hash: {}', tx_hash,
                            )
                        except Exception as ex:
                            self._logger.error(
                                'Unable to release epoch, error: {}', ex,
                            )

                            issue = GenericTxnIssue(
                                accountAddress=settings.validator_epoch_address,
                                epochBegin=epoch_block['begin'],
                                issueType='EpochReleaseError',
                                extra=str(ex),
                            )

                            await send_failure_notifications(client=self._client, message=issue)

                            # sleep for 30 seconds to avoid nonce collision
                            time.sleep(30)
                            # reset nonce
                            self._nonce = await w3.eth.get_transaction_count(
                                settings.validator_epoch_address,
                            )

                            last_contract_epoch = await self._fetch_epoch_from_contract()
                            if last_contract_epoch != -1:
                                begin_block_epoch = last_contract_epoch

                        await self._writer_redis_pool.set(
                            name=get_epoch_generator_last_epoch(),
                            value=epoch_block['end'],
                        )
                        await self._writer_redis_pool.zadd(
                            name=get_epoch_generator_epoch_history(),
                            mapping={
                                json.dumps({'begin': epoch_block['begin'], 'end': epoch_block['end']}): int(
                                    time.time(),
                                ),
                            },
                        )

                        epoch_generator_history_len = await self._writer_redis_pool.zcard(
                            get_epoch_generator_epoch_history(),
                        )

                        # Remove oldest epoch history if length exceeds configured limit
                        history_len = settings.chain.epoch.history_length
                        if epoch_generator_history_len > history_len:
                            await self._writer_redis_pool.zremrangebyrank(
                                get_epoch_generator_epoch_history(), 0,
                                -history_len,
                            )

                        self.last_sent_block = epoch_block['end']
                        self._logger.debug(
                            'Waiting to push next epoch in {} seconds...', sleep_secs_between_chunks,
                        )
                        # fixed wait
                        await asyncio.sleep(sleep_secs_between_chunks)
                    else:
                        begin_block_epoch = end_block_epoch + 1


def main():
    """Spin up the ticker process in event loop"""
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    ticker_process = EpochGenerator()
    loop.run_until_complete(ticker_process.run())


if __name__ == '__main__':
    main()
