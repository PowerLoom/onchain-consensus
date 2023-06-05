# Offchain Consensus

## Table of Contents

- [Overview](#overview)
- [Setup](#setup)
- [Development Instructions](#development-instructions)
- [Monitoring and Debugging](#monitoring-and-debugging)
- [Epoch Generation](#epoch-generation)
- [Using the Snapshotter CLI](#using-the-snapshotter-cli)
- [Running just Consensus service using Docker](#running-just-consensus-service-using-docker)

## Overview

Offchain Consensus is a temporary consensus layer for Powerloom Protocol. Its purpose is to demonstrate proof of concept while we develop the on-chain and fully decentralized consensus. Offchain Consensus is component of a fully functional, distributed system that works alongside Audit Protocol and Pooler. Together, these systems are responsible for the following:

- generating a time series database of changes occurring over smart contract state data and event logs that live on decentralized storage protocols
- higher order aggregated information calculated over decentralized indexes maintained atop the database mentioned above

Offchain Consensus performs the following functions:

1. Maintains and releases `Epoch` depending on chain and use case configuration
2. Receives snapshot submissions from all snapshotters and achieves consensus wherever possible
3. Provides a set of APIs for metrics and system state statistics

## Setup

Offchain Consensus alone is not of much use. If you're a snapshotter, you don't need to worry about this service as Powerloom provides `CONSENSUS_URL`, which you and other snapshotters will be using. But if you're a developer and want to play around with the system and build your use case, then you should follow [these instructions](https://github.com/PowerLoom/deploy#instructions-for-code-contributors) to set up the Powerloom System.

## Development Instructions

These instructions are needed if you're planning to run the system using `build-dev.sh` from [deploy](https://github.com/PowerLoom/deploy).

### Generate Config

The Offchain Consensus system needs the `settings.json` file to be present in the `settings` directory. We've provided `settings/settings.example.json` for you to get started. Changes are trivial. Copy `settings.example.json` to `settings.json` and make the necessary configuration changes.

#### Configuring settings.json
There are a lot of configuration in `settings.json` file, most of them are self explanatory but here are the few that are not
- `consensus_criteria.min_snapshotter_count` is the minimum number of submissions required to achieve consensus at the end of submission window if consensus is not yet achieved
- `consensus_criteria.percentage` is the percentage of snapshotters submitting the same snapshot to achieve consensus
- `ticker_begin_block` is the block from which you want epoch detector service to start (starts from current block if set to 0)
- `epoch.height` is the size of one epoch. This can depend on use case and chain in use
- `epoch.block_time` is the rough block time of current blockchain being used, for example for ETH mainnet you can use `15`

## Monitoring and Debugging

Login to Offchain Consensus Docker container using `docker exec -it <container_id> bash` (use `docker ps` to see running containers) and use the following commands for monitoring and debugging:

- To monitor the status of running processes, run `pm2 status`.
- To see all logs, run `pm2 logs`.
- To see logs for a specific process, run `pm2 logs <Process Identifier>`.
- To see only error logs, run `pm2 logs --err`.

Or you can simply use `docker logs -f offchain-consensus` if you don't want to go inside docker container.
## Epoch Generation

An epoch denotes a range of block heights on the data source blockchain, Ethereum mainnet in the case of Uniswap v2. This makes it easier to collect state transitions and snapshots of data on equally spaced block height intervals, as well as to support future work on other lightweight anchor proof mechanisms like Merkle proofs, succinct proofs, etc.

The size of an epoch is configurable. Let that be referred to as `size(E)`.

- A service keeps track of the head of the chain as it moves ahead, and a marker `h₀` against the max block height from the last released epoch. This makes the beginning of the next epoch, `h₁ = h₀ + 1`.
- Once the head of the chain has moved sufficiently ahead so that an epoch can be published, an epoch finalization service takes into account the following factors
    - chain reorganization reports where the reorganized limits are a subset of the epoch qualified to be published
    - a configurable ‘offset’ from the bleeding edge of the chain

 and then publishes an epoch `(h₁, h₂)` so that `h₂ - h₁ + 1 == size(E)`. The next epoch, therefore, is tracked from `h₂ + 1`.

## Using the Snapshotter CLI
Snapshotter CLI is a CLI interface to manage allowed snapshotters in the system. It has the following commands available

1. Add a snapshotter
```bash
poetry run python -m snapshotter_cli add-snapshotter '{"rate_limit": "10000/day;300/minute;40/second", "active": "active", "name": "HappySnapper", "email": "xyx@abc.com", "alias": "ALIAS"}'
```

2. Disable a snapshotter
```bash
python -m snapshotter_cli disable-snapshotter <ALIAS>
```

3. Enable a disabled snapshotter
```bash
python -m snapshotter_cli enable-snapshotter <ALIAS>
```

## Running just Consensus service using Docker
If you want to deploy consensus service for some reason, you can do so by following the following steps

- Build the image using `./build-docker.sh`
- Run the image using
```bash
docker rm -f offchain-consensus && docker run -p 9030:9030 --name offchain-consensus -d powerloom-offchain-consensus:latest && docker logs -f offchain-consensus
```
This will run consensus layer on port `9030` of your host.
### Consensus Dashboard
The UI dashboard for this is hosted at [ap-consensus-dashboard](https://github.com/PowerLoom/ap-consensus-dashboard), please follow the deploy instructions there to run the UI.
