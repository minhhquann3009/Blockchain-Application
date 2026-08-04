import asyncio

from src.crypto.signing import generate_keypair, pubkey_hex
from src.types.messages import Transaction
from src.network.simulator import Network, NetworkConfig
from src.node import Node


async def run_t1(num_nodes: int = 8, num_blocks: int = 3):
    keys = [generate_keypair() for _ in range(num_nodes)]
    validators = sorted(pubkey_hex(vk) for _, vk in keys)
    sk_by_pub = {pubkey_hex(vk): sk for sk, vk in keys}

    net = Network(NetworkConfig(stabilized=True, bounded_delay=0.02), log_path="logs/t1.log")
    nodes = {pub: Node(pub, sk_by_pub[pub], validators, net) for pub in validators}

    # Alice = validators[0] sets a value; submit to every node's mempool
    alice = validators[0]
    tx = Transaction(sender=alice, key=f"{alice}/message", value="hello", nonce=1)
    tx.sign(sk_by_pub[alice])
    for node in nodes.values():
        node.submit_tx(tx)

    tasks = [asyncio.create_task(node.run()) for node in nodes.values()]

    # let consensus run long enough to finalize num_blocks blocks
    await asyncio.sleep(2.0)

    for node in nodes.values():
        node.stop()
    net.flush_log()

    heights = {pub: len(node.ledger) for pub, node in nodes.items()}
    roots = {pub: (node.ledger[-1].block_hash() if node.ledger else None) for pub, node in nodes.items()}
    print("Heights per node:", heights)
    print("Last block hash per node (should all match):", set(roots.values()))
    assert len(set(roots.values())) == 1, "SAFETY VIOLATION: nodes disagree on finalized chain!"
    print("T1 PASSED: all nodes converged on the same finalized chain.")


if __name__ == "__main__":
    import os
    os.makedirs("logs", exist_ok=True)
    asyncio.run(run_t1())
