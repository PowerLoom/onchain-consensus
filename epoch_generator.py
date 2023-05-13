import asyncio
import json
import threading
import time
from functools import wraps
from multiprocessing import Process
from signal import SIGINT
from signal import signal
from signal import SIGQUIT
from signal import SIGTERM
from time import sleep

from redis import asyncio as aioredis
from setproctitle import setproctitle
from web3 import Web3

from exceptions import GenericExitOnSignal
from helpers.message_models import RPCNodesObject
from helpers.redis_keys import get_epoch_generator_epoch_history
from helpers.redis_keys import get_epoch_generator_last_epoch
from helpers.rpc_helper import ConstructRPC
from settings.conf import settings
from utils.default_logger import logger
from utils.redis_conn import RedisPool
from utils.transaction_utils import write_transaction

protocol_state_contract_address = settings.anchor_chain_rpc.protocol_state_address

# load abi from json file and create contract object
with open('utils/static/abi.json', 'r') as f:
    abi = json.load(f)
w3 = Web3(Web3.HTTPProvider(settings.anchor_chain_rpc.full_nodes[0].url))
protocol_state_contract = w3.eth.contract(
    address=settings.anchor_chain_rpc.protocol_state_address, abi=abi,
)


def chunks(start_idx, stop_idx, n):
    run_idx = 0
    for i in range(start_idx, stop_idx + 1, n):
        # Create an index range for l of n items:
        begin_idx = i  # if run_idx == 0 else i+1
        if begin_idx == stop_idx + 1:
            return
        end_idx = i + n - 1 if i + n - 1 <= stop_idx else stop_idx
        run_idx += 1
        yield begin_idx, end_idx, run_idx


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

    def __init__(self, name='PowerLoom|OffChainConsensus|EpochGenerator', simulation_mode=False):
        self.name = name
        setproctitle(self.name)
        self._logger = logger.bind(module=self.name)
        self._shutdown_initiated = False
        self.last_sent_block = 0
        self._end = None
        self.nonce = w3.eth.getTransactionCount(
            settings.anchor_chain_rpc.owner_address,
        )
        self.epochId = 1

    async def setup(self):
        self._aioredis_pool = RedisPool(writer_redis_conf=settings.redis)
        await self._aioredis_pool.populate()
        self._reader_redis_pool = self._aioredis_pool.reader_redis_pool
        self._writer_redis_pool = self._aioredis_pool.writer_redis_pool
        self.redis_thread: threading.Thread

    def _generic_exit_handler(self, signum, sigframe):
        if signum in [SIGINT, SIGTERM, SIGQUIT] and not self._shutdown_initiated:
            self._shutdown_initiated = True
            raise GenericExitOnSignal

    @redis_cleanup
    async def run(self):
        await self.setup()

        begin_block_epoch = settings.ticker_begin_block if settings.ticker_begin_block else 0
        for signame in [SIGINT, SIGTERM, SIGQUIT]:
            signal(signame, self._generic_exit_handler)
        last_epoch_data = protocol_state_contract.functions.currentEpoch().call()
        if last_epoch_data[1]:
            self._logger.debug(
                'Found last epoch block : {} in contract. Starting from checkpoint.', last_epoch_data[
                    1
                ],
            )
            begin_block_epoch = last_epoch_data[1] + 1
            self.epochId = last_epoch_data[2]
        else:
            self._logger.debug(
                'No last epoch block found in contract. Starting from configured block in settings.',
            )

        sleep_secs_between_chunks = 60

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
                sleep(settings.chain.epoch.block_time)
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
                    sleep(settings.chain.epoch.block_time)
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
                        time.sleep(
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

                        projects = protocol_state_contract.functions.getProjects().call()

                        self._logger.info(
                            'Force completing consensus for projects: {}', projects,
                        )
                        for project in projects:
                            try:
                                tx_hash = write_transaction(
                                    settings.anchor_chain_rpc.owner_address,
                                    settings.anchor_chain_rpc.owner_private_key,
                                    protocol_state_contract,
                                    'forceCompleteConsensusSnapshot',
                                    self.nonce,
                                    project,
                                    self.epochId,
                                )
                                self.nonce += 1
                                self._logger.info(
                                    'Force completing consensus for project: {}, txhash: {}', project, tx_hash,
                                )
                            except Exception as ex:
                                self._logger.error(
                                    'Unable to force complete consensus for project: {}, error: {}', project, ex,
                                )

                        try:
                            tx_hash = write_transaction(
                                settings.anchor_chain_rpc.owner_address,
                                settings.anchor_chain_rpc.owner_private_key,
                                protocol_state_contract,
                                'releaseEpoch',
                                self.nonce,
                                epoch_block['begin'],
                                epoch_block['end'],
                            )
                            self.nonce += 1
                            self.epochId += 1
                            self._logger.debug(
                                'Epoch Released! Transaction hash: {}', tx_hash,
                            )
                        except Exception as ex:
                            self._logger.error(
                                'Unable to release epoch, error: {}', ex,
                            )

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
                        sleep(sleep_secs_between_chunks)
                    else:
                        begin_block_epoch = end_block_epoch + 1


def main():
    """Spin up the ticker process in event loop"""
    ticker_process = EpochGenerator()
    asyncio.get_event_loop().run_until_complete(ticker_process.run())


if __name__ == '__main__':
    main()
