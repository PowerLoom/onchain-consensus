# Onchain Consensus

## Table of Contents

- [Onchain Consensus](#onchain-consensus)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Setup](#setup)
  - [Development Instructions](#development-instructions)
    - [Generate Config](#generate-config)
      - [Configuring settings.json](#configuring-settingsjson)
  - [Monitoring and Debugging](#monitoring-and-debugging)
  - [Epoch Generation](#epoch-generation)
  - [Force Consensus](#force-consensus)
  - [Running just Consensus service using Docker](#running-just-consensus-service-using-docker)
    - [Consensus Dashboard](#consensus-dashboard)

## Overview

![Overall Architecture](https://github.com/PowerLoom/pooler/raw/main/pooler/static/docs/assets/OverallArchitecture.png)
Onchain Consensus is part of the *Admin Module* in the overall architecture. It currently serves the following important roles -

1. Maintains and releases `Epoch` depending on chain and use case configuration
2. Checks and completes consensus (if necessary) by interacting with the Protocol State contract for previously released epochs after the submission window has passed
3. Provides a set of APIs for metrics and system state statistics where snapshotters can report their issues and overall network health can be monitored

## Setup

Onchain Consensus, currently, is only relevant to you if you're a validator. Snapshotters can just use the provided reporting URL in their configuration. But if you're a developer and want to play around with the system and build your use case, then you should follow [these instructions](https://github.com/PowerLoom/deploy#instructions-for-code-contributors) to set up the Powerloom System.

## Development Instructions

These instructions are needed if you're planning to run the system using `build-dev.sh` from [deploy](https://github.com/PowerLoom/deploy).

### Generate Config

The Onchain Consensus system needs the `settings.json` file to be present in the `settings` directory. We've provided `settings/settings.example.json` for you to get started. Changes are trivial. Copy `settings.example.json` to `settings.json` and make the necessary configuration changes.

#### Configuring settings.json

There are a lot of configurations in the `settings.json` file, most of them are self-explanatory but here are the few that are not
- `ticker_begin_block` is the block from which you want the `epoch_generator` service to start (starts from the current block if set to 0)
- `anchor_chain_rpc.url` is the RPC URL for Prost Chain where the protocol state lives
- `anchor_chain_rpc.protcol_state_address` is the Protocol State contract address with which `EpochGenerator` interacts and releases Epochs
- `anchor_chain_rpc.validator_epoch_address` is the EVM account address for the validator this is releasing/finalizing Epochs
- `anchor_chain_rpc.validator_epoch_private_key` is the validator EVM account address private key
- `anchor_chain_rpc.force_consensus_address` is the account address for the force consensus service, this doesn't need to be a validator account
- `anchor_chain_rpc.force_consensus_private_key` is the private key for the force consensus account

## Monitoring and Debugging

Login to the Onchain Consensus Docker container using `docker exec -it <container_id> bash` (use `docker ps` to see running containers) and use the following commands for monitoring and debugging:

- To monitor the status of running processes, run `pm2 status`.
- To see all logs, run `pm2 logs`.
- To see logs for a specific process, run `pm2 logs <Process Identifier>`.
- To see only error logs, run `pm2 logs --err`.

Or you can simply use `docker logs -f onchain-consensus` if you don't want to go inside the docker container.
## Epoch Generation

An epoch denotes a range of block heights on the data source blockchain, Ethereum mainnet in the case of Uniswap v2. This makes it easier to collect state transitions and snapshots of data on equally spaced block height intervals, as well as to support future work on other lightweight anchor proof mechanisms like Merkle proofs, succinct proofs, etc.

The size of an epoch is configurable. Let that be referred to as `size(E)`.

- A service keeps track of the head of the chain as it moves ahead, and a marker `h₀` against the max block height from the last released epoch. This makes the beginning of the next epoch, `h₁ = h₀ + 1`.
- Once the head of the chain has moved sufficiently ahead so that an epoch can be published, an epoch finalization service takes into account the following factors
    - chain reorganization reports where the reorganized limits are a subset of the epoch qualified to be published
    - a configurable ‘offset’ from the bleeding edge of the chain

 and then publishes an epoch `(h₁, h₂)` so that `h₂ - h₁ + 1 == size(E)`. The next epoch, therefore, is tracked from `h₂ + 1`.

The Epoch Release process is explained in detail in the sequence diagram below
![Epoch Generator Sequence Diagram](/docs/images/epoch_generator.png)

## Force Consensus

Force consensus is an optional mechanism that can be run by anyone in the network and is designed to trigger consensus checks for projects that didn't reach consensus automatically with a 51% majority within the submission window. This will force consensus if possible if the project submissions meet all internal criteria for consensus after the submission window is closed.

Force Consensus works slightly differently than Epoch Generator and is heavily optimized to handle a lot of projects. The sequence diagram explaining the flow is given below
![Force Consensus Sequence Diagram](/docs/images/force_consensus.png)

Transaction tasks are then processed parallelly using the following flow
![Force Consensus Transaction Task Processing](/docs/images/txn_task.png)
## Running just Consensus service using Docker
If you want to deploy consensus service for some reason, you can do so by following the following steps:

- Build the image using `./build-docker.sh`
- Run the image using
```bash
 docker rm -f onchain-consensus && docker run --add-host host.docker.internal:host-gateway -p 8080:8080 --name onchain-consensus -d powerloom-onchain-consensus:latest && docker logs -f onchain-consensus
 ```
This will run the consensus layer on port `9030` of your host.
### Consensus Dashboard
The UI dashboard for this is hosted at [ap-consensus-dashboard](https://github.com/PowerLoom/ap-consensus-dashboard), please follow the deploy instructions there to run the UI.
