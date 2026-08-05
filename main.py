import asyncio

from nacl.signing import VerifyKey, SigningKey
from src.crypto.signing import generate_keypair, pubkey_hex
from src.types.messages import Transaction
from src.network.simulator import Network, NetworkConfig
from src.node import Node


STATE_FORMAT = "{username}/message"


def create_network(log_path: str) -> Network:
    network_config = NetworkConfig(
        stabilized=True,
        bounded_delay=0.02
    )
    network = Network(
        network_config,
        log_path=log_path,
    )
    return network


def create_nodes(num_nodes: int, network: Network) -> dict[str, Node]:
    validators = [generate_keypair() for _ in range(num_nodes)]
    sorted_node_ids = sorted(pubkey_hex(verify_key) for _, verify_key in validators)

    node_lookup = {}
    for sign_key, verify_key in validators:
        node_id = pubkey_hex(verify_key)
        node_lookup[node_id] = Node(
            node_id=node_id,
            signing_key=sign_key,
            validators=sorted_node_ids,  # Sorted for consistent proposer in consensus round
            network=network,
        )
    return node_lookup


def make_transaction(
        username: str, value: str,
        account: tuple[SigningKey, VerifyKey]
    ) -> Transaction:
    tx = Transaction(
        sender=pubkey_hex(account[1]),
        key=STATE_FORMAT.format(username=username),
        value=value, 
        nonce=1,
    )
    tx.sign(account[0])
    return tx


async def run_t1():
    NUM_NODES = 4
    NUM_ACCOUNTS = 1
    LOG_PATH = "logs/t1.jsonl"

    accounts = [generate_keypair() for _ in range(NUM_ACCOUNTS)]
    network = create_network(LOG_PATH)
    node_lookup = create_nodes(NUM_NODES, network)

    # Make a tx
    acc_00 = accounts[0]
    tx = make_transaction(
        username="acc_00",
        value="Hi, I'm Alice!", 
        account=acc_00,
    )
    # Broadcast the tx to network
    for node in node_lookup.values():
        node.submit_tx(tx)

    # Start simulation
    tasks = [asyncio.create_task(node.run()) for node in node_lookup.values()]    
    # Let consensus run long enough to finalize block
    await asyncio.sleep(4.0)
    # End simulation
    for node in node_lookup.values():
        node.stop()

    network.flush_log()

    all_node_heights = {node_id: len(node.ledger) for node_id, node in node_lookup.items()}
    all_node_lasthashs = {node_id: (node.ledger[-1].block_hash() if node.ledger else None) for node_id, node in node_lookup.items()}
    print("Heights per node:", all_node_heights)
    print("Last block hash per node (should all match):", set(all_node_lasthashs.values()))
    assert len(set(all_node_lasthashs.values())) == 1, "SAFETY VIOLATION: nodes disagree on finalized chain!"
    print("T1 PASSED: all nodes converged on the same finalized chain.")


if __name__ == "__main__":
    import os
    os.makedirs("logs", exist_ok=True)
    asyncio.run(run_t1())
