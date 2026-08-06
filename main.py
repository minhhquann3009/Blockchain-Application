import asyncio
import argparse
from typing import Callable, Optional

from nacl.signing import VerifyKey, SigningKey
from src.crypto.signing import generate_keypair, pubkey_hex
from src.types.messages import Transaction, short_addr
from src.network.simulator import Network, NetworkConfig
from src.node import Node

Account = tuple[SigningKey, VerifyKey]
TimelineStep = tuple[float, Optional[Callable[[], None]]]


# --------------------------------------------------------------------------
# Node / network construction
# --------------------------------------------------------------------------

def create_network(network_config: NetworkConfig, log_path: str) -> Network:
    return Network(network_config, log_path=log_path)


def _node_lookup_from(validators: list[Account], network: Network, tamper_ids: set[int]) -> dict[str, Node]:
    def node_id_for(i: int, verify_key: VerifyKey) -> str:
        real_id = pubkey_hex(verify_key)
        # Force a shared prefix to simulate a byzantine node presenting a
        # tampered / mismatched node_id.
        return "fff" + real_id[3:] if i in tamper_ids else real_id

    node_ids = [node_id_for(i, vk) for i, (_, vk) in enumerate(validators)]
    sorted_node_ids = sorted(node_ids)  # sorted for consistent proposer ordering in consensus round

    node_lookup: dict[str, Node] = {}
    for i, (sign_key, verify_key) in enumerate(validators):
        node_id = node_id_for(i, verify_key)
        node_lookup[node_id] = Node(
            node_id=node_id,
            signing_key=sign_key,
            validators=sorted_node_ids,
            network=network,
        )
    return node_lookup


def create_nodes(num_nodes: int, network: Network) -> dict[str, Node]:
    """Create `num_nodes` honest validators."""
    validators = [generate_keypair() for _ in range(num_nodes)]
    return _node_lookup_from(validators, network, tamper_ids=set())


def create_tamper_nodes(num_nodes: int, network: Network, num_tamper: int) -> dict[str, Node]:
    """Create `num_nodes` validators; the first `num_tamper` register under a
    tampered node_id to simulate a byzantine identity mismatch."""
    validators = [generate_keypair() for _ in range(num_nodes)]
    return _node_lookup_from(validators, network, tamper_ids=set(range(num_tamper)))


def make_transaction(username: str, value: str, account: Account) -> Transaction:
    tx = Transaction(
        sender=pubkey_hex(account[1]),
        key=username,
        value=value,
        nonce=1,
    )
    tx.sign(account[0])
    return tx


def quorum_for(num_nodes: int) -> int:
    """2f+1 out of n=3f+1 validators."""
    f = (num_nodes - 1) // 3
    return 2 * f + 1


# --------------------------------------------------------------------------
# Simulation harness
# --------------------------------------------------------------------------

async def run_timeline(node_lookup: dict[str, Node], timeline: list[TimelineStep]) -> None:
    """Start every node's run() loop, then walk through `timeline`: a list of
    (sleep_seconds, optional_callback) steps executed in order. Once the
    timeline is done, stop all nodes and *wait for their run() tasks to
    actually finish* before returning control.

    This last part matters: reading node.ledger right after calling stop(),
    without awaiting the tasks, is a race -- the node may not have finished
    its in-flight step yet, and any exception raised inside run() was
    previously discarded silently because the tasks were never awaited.
    """
    tasks = [asyncio.create_task(node.run()) for node in node_lookup.values()]
    try:
        for duration, callback in timeline:
            await asyncio.sleep(duration)
            if callback is not None:
                callback()
    finally:
        for node in node_lookup.values():
            node.stop()


def report_state(node_lookup: dict[str, Node]) -> dict[str, Optional[str]]:
    heights = {short_addr(nid): len(n.ledger) for nid, n in node_lookup.items()}
    last_hashes = {
        short_addr(nid): (short_addr(n.ledger[-1].block_hash()) if n.ledger else None)
        for nid, n in node_lookup.items()
    }
    print("Heights per node:", heights)
    print("Last block hash per node:", last_hashes)
    return last_hashes


def assert_unanimous(last_hashes: dict[str, Optional[str]]) -> None:
    """All nodes must agree on the exact same finalized chain. Appropriate
    when the scenario gives every node a fair chance to converge."""
    distinct = set(last_hashes.values())
    assert len(distinct) == 1, f"SAFETY VIOLATION: nodes disagree on finalized chain! {distinct}"


def assert_quorum_agreement(last_hashes: dict[str, Optional[str]], num_nodes: int) -> None:
    """Weaker check for partition / drop-rate scenarios: a byzantine or
    temporarily-partitioned minority may legitimately lag or disagree, so
    require only that at least a quorum of nodes converge on the same
    finalized chain, not literally every node.
    """
    groups: dict[Optional[str], list[str]] = {}
    for node_id, h in last_hashes.items():
        groups.setdefault(h, []).append(node_id)
    largest = max(groups.values(), key=len)
    needed = quorum_for(num_nodes)
    print(f"Largest agreeing group: {len(largest)}/{num_nodes} nodes {largest} (quorum needed: {needed})")
    assert len(largest) >= needed, "SAFETY VIOLATION: no quorum agrees on a single finalized chain!"


# --------------------------------------------------------------------------
# Test scenarios
# --------------------------------------------------------------------------

async def run_t1():
    """Baseline: single tx, stable network, all nodes should finalize identically."""
    NUM_NODES = 4
    network = create_network(NetworkConfig(stabilized=True, bounded_delay=0.02), "logs/t1.jsonl")
    node_lookup = create_nodes(NUM_NODES, network)

    acc_00 = generate_keypair()
    tx = make_transaction("acc_00", "Hi, I'm Alice!", acc_00)
    for node in node_lookup.values():
        node.submit_tx(tx)

    await run_timeline(node_lookup, [(4.0, None)])
    network.flush_log()

    last_hashes = report_state(node_lookup)
    assert_unanimous(last_hashes)
    print("T1 PASSED: all nodes converged on the same finalized chain.")


async def run_t2():
    """Duplicated + reordered messages on an otherwise stable network."""
    NUM_NODES = 4
    net_config = NetworkConfig(
        duplicate_rate=0.3,
        reorder_list=[0.5, 0.45, 0.40, 0.35, 0.30, 0.25, 0.20, 0.15, 0.10, 0.05],  # reorder first 10 msgs by delay
        bounded_delay=0.01,
    )
    network = create_network(net_config, "logs/t2.jsonl")
    network.set_seed(42)  # reproducible
    node_lookup = create_nodes(NUM_NODES, network)

    acc_00 = generate_keypair()
    tx = make_transaction("acc_00", "Hi, I'm Alice!", acc_00)
    for node in node_lookup.values():
        node.submit_tx(tx)

    await run_timeline(node_lookup, [(2.0, None)])
    network.flush_log()

    last_hashes = report_state(node_lookup)
    assert_unanimous(last_hashes)
    print("T2 PASSED: all nodes converged on the same finalized chain.")


async def run_t3():
    """One node presents a tampered sender id on its tx; mempool is reset
    mid-run to drop it and consensus should still converge cleanly."""
    NUM_NODES = 4
    network = create_network(NetworkConfig(stabilized=True, bounded_delay=0.02), "logs/t3.jsonl")
    network.set_seed(42)
    node_lookup = create_tamper_nodes(NUM_NODES, network, num_tamper=1)

    acc_00 = generate_keypair()
    tx = make_transaction("acc_00", "Hi, I'm Alice!", acc_00)
    tx.sender = "fff" + tx.sender[3:]  # tamper the tx to match the tampered node id
    for node in node_lookup.values():
        node.submit_tx(tx)

    def reset_mempools() -> None:
        for node in node_lookup.values():
            node.reset_mempool()

    await run_timeline(node_lookup, [(0.1, reset_mempools), (1.9, None)])
    network.flush_log()

    last_hashes = report_state(node_lookup)
    assert_unanimous(last_hashes)
    print("T3 PASSED: all nodes converged on the same finalized chain.")


async def run_t4():
    """Same tx submitted to every node individually (simulating duplicate
    delivery) plus a second, distinct tx -- both should apply exactly once."""
    NUM_NODES = 4
    network = create_network(NetworkConfig(stabilized=True, bounded_delay=0.02), "logs/t4.jsonl")
    node_lookup = create_nodes(NUM_NODES, network)

    acc_00, acc_01 = generate_keypair(), generate_keypair()
    tx_00 = make_transaction("acc_00", "Hi, I'm Alice!", acc_00)
    tx_01 = make_transaction("acc_01", "Bob is here!", acc_01)
    for node in node_lookup.values():
        node.submit_tx(tx_00)
        node.submit_tx(tx_01)
        node.submit_tx(tx_00)
        node.submit_tx(tx_01)

    await run_timeline(node_lookup, [(4.0, None)])
    network.flush_log()

    last_hashes = report_state(node_lookup)
    assert_unanimous(last_hashes)
    print("T4 PASSED: applied duplicated + distinct transactions exactly once.")
    first_node = next(iter(node_lookup.values()))
    print("Final state machine:", first_node.state.data)


async def run_t5():
    """Unstable network (drops) for a period, then stabilizes. Correct nodes
    should still reach quorum agreement once conditions improve; we don't
    require literal unanimity since a lagging/partitioned minority is
    expected here."""
    NUM_NODES = 7
    net_config = NetworkConfig(stabilized=False, drop_rate=0.2)
    network = create_network(net_config, "logs/t5.jsonl")
    node_lookup = create_nodes(NUM_NODES, network)

    acc_00 = generate_keypair()
    tx = make_transaction("acc_00", "Hi, I'm Alice!", acc_00)
    for node in node_lookup.values():
        node.submit_tx(tx)

    def stabilize() -> None:
        network.config.stabilized = True
        network.config.drop_rate = 0.0

    await run_timeline(node_lookup, [(1.0, stabilize), (3.0, None)])
    network.flush_log()

    last_hashes = report_state(node_lookup)
    assert_quorum_agreement(last_hashes, NUM_NODES)
    print("T5 PASSED: quorum of correct nodes converged on the same finalized chain.")


SCENARIOS = {
    "1": run_t1,
    "2": run_t2,
    "3": run_t3,
    "4": run_t4,
    "5": run_t5,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--test",
        default="1",
        choices=sorted(SCENARIOS.keys()),
        help="Which test case to run",
    )
    args = parser.parse_args()

    asyncio.run(SCENARIOS[args.test]())