import asyncio
import json
import random
import threading
import time

import aiorwlock
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
from helpers.redis_keys import event_detector_last_processed_block
from rpc import get_event_sig_and_abi
from rpc import RpcHelper
from settings.conf import settings
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
    address=protocol_state_contract_address, abi=abi,
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
        self._epochId = 1
        self._pending_epochs = set()
        self._submission_window = 0
        self._semaphore = asyncio.Semaphore(value=20)
        self._nonce = -1
        self.rpc_helper = RpcHelper(rpc_settings=settings.anchor_chain.rpc)
        self._last_processed_block = 0

        EVENTS_ABI = {
            'EpochReleased': protocol_state_contract.events.EpochReleased._get_event_abi(),
        }

        EVENT_SIGS = {
            'EpochReleased': 'EpochReleased(uint256,uint256,uint256,uint256)',
        }

        self.event_sig, self.event_abi = get_event_sig_and_abi(
            EVENT_SIGS,
            EVENTS_ABI,
        )

    async def get_events(self, from_block: int, to_block: int):
        """Get the events from the block range.

        Arguments:
            int : from block
            int: to block

        Returns:
            list : (type, event)
        """
        events_log = await self.rpc_helper.get_events_logs(
            **{
                'contract_address': protocol_state_contract_address,
                'to_block': to_block,
                'from_block': from_block,
                'topics': [self.event_sig],
                'event_abi': self.event_abi,
                'redis_conn': self._writer_redis_pool,
            },
        )
        for log in events_log:
            if log['event'] == 'EpochReleased':
                self._pending_epochs.add((time.time(), log['args']['epochId']))

        asyncio.ensure_future(self._force_complete_consensus())

    async def setup(self):
        self._aioredis_pool = RedisPool(writer_redis_conf=settings.redis)
        self._nonce = await w3.eth.get_transaction_count(
            settings.validator_epoch_address,
        )

        await self._aioredis_pool.populate()
        self._reader_redis_pool = self._aioredis_pool.reader_redis_pool
        self._writer_redis_pool = self._aioredis_pool.writer_redis_pool
        self.redis_thread: threading.Thread

        if not self._rwlock:
            self._rwlock = aiorwlock.RWLock()

        self._nonce = await w3.eth.get_transaction_count(
            settings.force_consensus_address,
        )
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
            timeout=Timeout(timeout=5.0),
            follow_redirects=False,
            transport=self._async_transport,
        )

    async def _call_force_complete_consensus(self, project, epochId):
        async with self._semaphore:
            if await protocol_state_contract.functions.checkDynamicConsensusSnapshot(
                project, epochId,
            ).call():

                try:
                    async with self._rwlock.writer_lock:
                        rand = random.random()
                        # check receipt and heal if needed 1% of the time
                        if rand < 0.01:
                            tx_hash, receipt = await write_transaction_with_receipt(
                                w3,
                                settings.force_consensus_address,
                                settings.force_consensus_private_key,
                                protocol_state_contract,
                                'forceCompleteConsensusSnapshot',
                                self._nonce,
                                project,
                                epochId,
                            )

                            if receipt['status'] != 1:
                                self._logger.error(
                                    'Unable to force complete consensus for project: {}, error: {}',
                                )

                                issue = GenericTxnIssue(
                                    accountAddress=settings.force_consensus_address,
                                    epochId=epochId,
                                    issueType='ForceConsensusTxnFailed',
                                    projectId=project,
                                    extra=json.dumps(receipt),
                                )

                                await send_failure_notifications(
                                    client=self._client,
                                    issue=issue,
                                )

                                time.sleep(5)
                                self._nonce = await w3.eth.get_transaction_count(
                                    settings.force_consensus_address,
                                )
                                return
                        else:

                            tx_hash = await write_transaction(
                                w3,
                                settings.force_consensus_address,
                                settings.force_consensus_private_key,
                                protocol_state_contract,
                                'forceCompleteConsensusSnapshot',
                                self._nonce,
                                project,
                                epochId,
                            )
                    self._nonce += 1
                    self._logger.info(
                        'Force completing consensus for project: {}, epoch: {}, txhash: {}', project, epochId, tx_hash,
                    )
                except Exception as ex:
                    self._logger.error(
                        'Unable to force complete consensus for project: {}, error: {}', project, ex,
                    )

                    issue = GenericTxnIssue(
                        accountAddress=settings.force_consensus_address,
                        epochId=epochId,
                        issueType='ForceConsensusTxnFailed',
                        projectId=project,
                        extra=str(ex),
                    )

                    await send_failure_notifications(
                        client=self._client,
                        issue=issue,
                    )
                    # reset nonce
                    async with self._rwlock.writer_lock:
                        # sleep for 5 seconds to avoid nonce collision

                        time.sleep(5)
                        self._nonce = await w3.eth.get_transaction_count(
                            settings.force_consensus_address,
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

    async def run(self):

        await self.setup()

        if self._submission_window == 0:
            self._submission_window = await protocol_state_contract.functions.snapshotSubmissionWindow().call()

        while True:
            try:
                current_block = await self.rpc_helper.get_current_block(redis_conn=self._writer_redis_pool)
                self._logger.info('Current block: {}', current_block)

            except Exception as e:
                self._logger.opt(exception=True).error(
                    (
                        'Unable to fetch current block, ERROR: {}, '
                        'sleeping for {} seconds.'
                    ),
                    e,
                    settings.anchor_chain.polling_interval,
                )

                await asyncio.sleep(settings.anchor_chain.polling_interval)
                continue

            # Only use redis is state is not locally present
            if not self._last_processed_block:
                last_processed_block_data = await self._reader_redis_pool.get(
                    event_detector_last_processed_block,
                )

                if last_processed_block_data:
                    self._last_processed_block = json.loads(
                        last_processed_block_data,
                    )

            if self._last_processed_block:
                if current_block - self._last_processed_block >= 10:
                    self._logger.warning(
                        'Last processed block is too far behind current block, '
                        'processing current block',
                    )
                    self._last_processed_block = current_block - 10

                # Get events from current block to last_processed_block
                try:
                    await self.get_events(self._last_processed_block, current_block)
                except Exception as e:
                    self._logger.opt(exception=True).error(
                        (
                            'Unable to fetch events from block {} to block {}, '
                            'ERROR: {}, sleeping for {} seconds.'
                        ),
                        self._last_processed_block + 1,
                        current_block,
                        e,
                        settings.anchor_chain.polling_interval,
                    )
                    await asyncio.sleep(settings.anchor_chain.polling_interval)
                    continue

            else:

                self._logger.debug(
                    'No last processed epoch found, processing current block',
                )

                try:
                    await self.get_events(current_block, current_block)
                except Exception as e:
                    self._logger.opt(exception=True).error(
                        (
                            'Unable to fetch events from block {} to block {}, '
                            'ERROR: {}, sleeping for {} seconds.'
                        ),
                        current_block,
                        current_block,
                        e,
                        settings.anchor_chain.polling_interval,
                    )
                    await asyncio.sleep(settings.anchor_chain.polling_interval)
                    continue

            self._last_processed_block = current_block

            await self._writer_redis_pool.set(event_detector_last_processed_block, json.dumps(current_block))
            self._logger.info(
                'DONE: Processed blocks till, saving in redis: {}',
                current_block,
            )
            self._logger.info(
                'Sleeping for {} seconds...',
                settings.anchor_chain.polling_interval,
            )
            await asyncio.sleep(settings.anchor_chain.polling_interval)


def main():
    """Spin up the ticker process in event loop"""
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    force_consensus_process = ForceConsensus()
    loop.run_until_complete(force_consensus_process.run())


if __name__ == '__main__':
    main()
