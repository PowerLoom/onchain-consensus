from web3 import Web3

from settings.conf import settings


w3 = Web3(Web3.HTTPProvider(settings.anchor_chain_rpc.full_nodes[0].url))

CHAIN_ID = settings.anchor_chain_rpc.chain_id


def write_transaction(address, private_key, contract, function, nonce, *args):
    """ Writes a transaction to the blockchain

    Args:
            address (str): The address of the account
            private_key (str): The private key of the account
            contract (web3.eth.contract): Web3 contract object
            function (str): The function to call
            *args: The arguments to pass to the function

    Returns:
            str: The transaction hash
    """
    # Create the function
    func = getattr(contract.functions, function)
    # Get the transaction
    transaction = func(*args).buildTransaction({
        'from': address,
        'gas': 2000000,
        'gasPrice': w3.toWei('0.0001', 'gwei'),
        'nonce': nonce,
        'chainId': CHAIN_ID,
    })
    # Sign the transaction
    signed_transaction = w3.eth.account.sign_transaction(
        transaction, private_key=private_key,
    )
    # Send the transaction
    tx_hash = w3.eth.send_raw_transaction(signed_transaction.rawTransaction)
    # Wait for confirmation
    return tx_hash.hex()


def write_transaction_with_receipt(address, private_key, contract, function, nonce, *args):
    """ Writes a transaction using write_transaction, wait for confirmation and retry doubling gas price if failed

    Args:
        address (str): The address of the account
        private_key (str): The private key of the account
        contract (web3.eth.contract): Web3 contract object
        function (str): The function to call
        *args: The arguments to pass to the function

    Returns:
        str: The transaction hash
    """
    tx_hash = write_transaction(
        address, private_key, contract, function, nonce, *args,
    )

    # Wait for confirmation
    receipt = w3.eth.waitForTransactionReceipt(tx_hash)
    return tx_hash, receipt
