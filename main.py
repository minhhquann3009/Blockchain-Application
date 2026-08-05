import asyncio
import argparse
from nacl.signing import VerifyKey, SigningKey
from src.crypto.signing import generate_keypair, pubkey_hex
from src.types.messages import Transaction
from src.network.simulator import Network, NetworkConfig
from src.node import Node


STATE_FORMAT = "{username}/message"


def create_network(network_config: NetworkConfig, log_path: str) -> Network:
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
    NET_CONFIG = NetworkConfig(
        stabilized=True,
        bounded_delay=0.02
    )
    LOG_PATH = "logs/t1.jsonl"


    accounts = [generate_keypair() for _ in range(NUM_ACCOUNTS)]
    network = create_network(NET_CONFIG, LOG_PATH)
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


async def run_t2():
    NUM_NODES = 4
    NUM_ACCOUNTS = 1
    NET_CONFIG = NetworkConfig(
        duplicate_rate=0.3,
        reorder_list=[0.5, 0.45, 0.40, 0.35, 0.30, 0.25, 0.20, 0.15, 0.10, 0.05],  # Reorder first 10 msg by delay time
        bounded_delay=0.01,
    )
    RANDOM_SEED = 42
    LOG_PATH = "logs/t2.jsonl"


    accounts = [generate_keypair() for _ in range(NUM_ACCOUNTS)]
    network = create_network(NET_CONFIG, LOG_PATH)
    node_lookup = create_nodes(NUM_NODES, network)

    # Repoducible
    network.set_seed(RANDOM_SEED)

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
    await asyncio.sleep(2.0)
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", default=1, help="Run test cases (1-8)")
    args = parser.parse_args()

    test_id = args.test
    match test_id:
        case '1':
            asyncio.run(run_t1())
        case '2':
            asyncio.run(run_t2())
        case _:
            print("Empty test case!")

