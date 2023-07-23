import asyncio
import json
import threading
import time
from multiprocessing import Process

import aiorwlock
import uvloop
from redis import asyncio as aioredis
from setproctitle import setproctitle
from web3 import AsyncHTTPProvider
from web3 import AsyncWeb3

from helpers.message_models import RPCNodesObject
from helpers.rpc_helper import ConstructRPC
from settings.conf import settings
from utils.chunk_helper import chunks
from utils.default_logger import logger
from utils.redis_conn import RedisPool
from utils.transaction_utils import write_transaction

protocol_state_contract_address = settings.anchor_chain_rpc.protocol_state_address

# load abi from json file and create contract object
with open('utils/static/abi.json', 'r') as f:
    abi = json.load(f)
# w3 = Web3(Web3.HTTPProvider(settings.anchor_chain_rpc.full_nodes[0].url))

w3 = AsyncWeb3(AsyncHTTPProvider(settings.anchor_chain_rpc.full_nodes[0].url))

protocol_state_contract = w3.eth.contract(
    address=settings.anchor_chain_rpc.protocol_state_address, abi=abi,
)


class ForceConsensus:
    _aioredis_pool: RedisPool
    _reader_redis_pool: aioredis.Redis
    _writer_redis_pool: aioredis.Redis

    def __init__(self, name='PowerLoom|OnChainConsensus|ForceConsensus'):
        self.name = name
        setproctitle(self.name)
        self._logger = logger.bind(module=self.name)
        self._shutdown_initiated = False
        self.last_sent_block = 0
        self._end = None
        self._rwlock = None
        self.epochId = 1
        self._pending_epochs = set()
        self._submission_window = 0
        self._semaphore = asyncio.Semaphore(value=20)
        self.nonce = -1

    async def setup(self):

        if not self._rwlock:
            self._rwlock = aiorwlock.RWLock()

        self.nonce = await w3.eth.get_transaction_count(
            settings.anchor_chain_rpc.validator_consensus_address,
        )

        self._aioredis_pool = RedisPool(writer_redis_conf=settings.redis)
        await self._aioredis_pool.populate()
        self._reader_redis_pool = self._aioredis_pool.reader_redis_pool
        self._writer_redis_pool = self._aioredis_pool.writer_redis_pool
        self.redis_thread: threading.Thread

    async def _call_force_complete_consensus(self, project, epochId):
        async with self._semaphore:
            if await protocol_state_contract.functions.checkDynamicConsensusSnapshot(
                project, epochId,
            ).call():
                try:
                    async with self._rwlock.writer_lock:
                        tx_hash = await write_transaction(
                            w3,
                            settings.anchor_chain_rpc.validator_consensus_address,
                            settings.anchor_chain_rpc.validator_consensus_private_key,
                            protocol_state_contract,
                            'forceCompleteConsensusSnapshot',
                            self.nonce,
                            project,
                            epochId,
                        )
                        self.nonce += 1
                    self._logger.info(
                        'Force completing consensus for project: {}, txhash: {}', project, tx_hash,
                    )
                except Exception as ex:
                    self._logger.error(
                        'Unable to force complete consensus for project: {}, error: {}', project, ex,
                    )
                    # reset nonce
                    async with self._rwlock.writer_lock:
                        # sleep for 5 seconds to avoid nonce collision
                        await asyncio.sleep(5)
                        self.nonce = await w3.eth.get_transaction_count(
                            settings.anchor_chain_rpc.validator_consensus_address,
                        )
            else:
                self._logger.info(
                    'Consensus already achieved for project: {}', project,
                )

    async def _force_complete_consensus(self):
        epochs_to_process = []
        epochs_to_remove = set()
        for release_time, epoch in self._pending_epochs:
            # anchor chain block time is 2 but using 2.5 for additional buffer
            if release_time + (self._submission_window * 2.5) < time.time():
                epochs_to_process.append(epoch)
                epochs_to_remove.add((release_time, epoch))

        self._pending_epochs -= epochs_to_remove

        self._logger.info('Processing Epochs {}', epochs_to_process)
        if epochs_to_process:
            projects = await protocol_state_contract.functions.getProjects().call()
            self._logger.info(
                'Force completing consensus for projects: {}', projects,
            )

            txn_tasks = []
            for epochId in epochs_to_process:
                for project in projects:
                    txn_tasks.append(self._call_force_complete_consensus(project, epochId))

            results = await asyncio.gather(*txn_tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    self._logger.error(
                        'Error while force completing consensus: {}', result,
                    )

    async def _fetch_epoch_from_contract(self) -> int:
        last_epoch_data = await protocol_state_contract.functions.currentEpoch().call()
        if last_epoch_data[1]:
            self._logger.debug(
                'Found last epoch block : {} in contract. Starting from checkpoint.', last_epoch_data[
                    1
                ],
            )
            begin_block_epoch = last_epoch_data[1] + 1
            self.epochId = last_epoch_data[2]
            return begin_block_epoch
        else:
            self._logger.debug(
                'No last epoch block found in contract. Starting from configured block in settings.',
            )
            return -1

    async def run(self):
        await self.setup()

        if self._submission_window == 0:
            self._submission_window = await protocol_state_contract.functions.snapshotSubmissionWindow().call()

        begin_block_epoch = settings.ticker_begin_block if settings.ticker_begin_block else 0

        last_contract_epoch = await self._fetch_epoch_from_contract()
        if last_contract_epoch != -1:
            begin_block_epoch = last_contract_epoch

        # waiting to release epoch chunks every half of block time
        sleep_secs_between_chunks = settings.chain.epoch.block_time // 4

        rpc_obj = ConstructRPC(network_id=settings.chain.chain_id)
        rpc_urls = []
        for node in settings.chain.rpc.nodes:
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

                        if epoch_block['end'] != epoch_block['begin']:
                            self._logger.error('Only epoch of length 1 is supported')
                            return
                        self._pending_epochs.add((time.time(), epoch_block['end']))

                        asyncio.create_task(self._force_complete_consensus())
                        await asyncio.sleep(sleep_secs_between_chunks)


def main():
    """Spin up the ticker process in event loop"""
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    force_consensus_process = ForceConsensus()
    loop.run_until_complete(force_consensus_process.run())


if __name__ == '__main__':
    main()
