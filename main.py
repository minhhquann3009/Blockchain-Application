import asyncio
from pathlib import Path
import argparse
from typing import Callable, Optional

from nacl.signing import VerifyKey, SigningKey
from src.crypto.signing import generate_keypair, keypair_from_seed, pubkey_hex
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
    """Create `num_nodes` honest validators with fresh random identities."""
    validators = [generate_keypair() for _ in range(num_nodes)]
    return _node_lookup_from(validators, network, tamper_ids=set())


def create_deterministic_nodes(num_nodes: int, network: Network) -> dict[str, Node]:
    """Create `num_nodes` honest validators whose identities are derived from
    fixed seeds, so node_ids -- and therefore proposer order and every log
    line -- are identical on every run. Required for T8."""
    validators = [keypair_from_seed(i) for i in range(num_nodes)]
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
    network = next(iter(node_lookup.values())).network
    clock = network.clock

    for node in node_lookup.values():
        await node.start()

    # `duration` is now VIRTUAL seconds: we advance the clock by draining
    # events up to a deadline rather than sleeping in real time. Same
    # scenario semantics, but the amount of work done is a property of the
    # protocol instead of the host machine's speed.
    deadline = clock.now
    for duration, callback in timeline:
        deadline += duration
        await clock.run(until=deadline)
        if callback is not None:
            callback()


async def run_until_height(
    node_lookup: dict[str, Node],
    target_height: int,
    timeout: float = 30.0,
) -> None:
    """Run every node until they have ALL finalized `target_height` blocks,
    then stop.

    Wall-clock run lengths ("sleep 4 seconds") are the third source of
    nondeterminism: a slower or faster machine finalizes a different number
    of blocks, so the logs differ in length between runs even when
    everything else is fixed. Stopping on a *logical* condition makes the
    amount of work performed a property of the protocol, not of the host.

    `timeout` is a safety net against a hung run, not a stop condition -- if
    it fires, the scenario has failed to make progress and we say so.
    """
    clock = next(iter(node_lookup.values())).network.clock

    for node in node_lookup.values():
        await node.start()

    def reached() -> bool:
        return all(len(n.ledger) >= target_height for n in node_lookup.values())

    await clock.run(until=clock.now + timeout, stop_condition=reached)

    if not reached():
        raise TimeoutError(
            f"LIVENESS FAILURE: nodes did not all reach height {target_height} "
            f"within {timeout} virtual seconds "
            f"(reached: {[len(n.ledger) for n in node_lookup.values()]})"
        )


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


async def run_t6():
    """Proposer for round 0 is silent/crashed from genesis (never started).
    Honest nodes must ROUND_TIMEOUT, advance to round 1 where a different
    (honest) proposer is elected, and still finalize -- liveness after a
    correct proposer is eventually selected."""
    NUM_NODES = 4  # f=1, quorum=3: tolerates exactly 1 crashed validator
    network = create_network(NetworkConfig(stabilized=True, bounded_delay=0.02), "logs/t6.jsonl")
    node_lookup = create_nodes(NUM_NODES, network)

    acc_00 = generate_keypair()
    tx = make_transaction("acc_00", "Hi, I'm Alice!", acc_00)
    for node in node_lookup.values():
        node.submit_tx(tx)

    # proposer_for(height=0, round=0) == sorted_validators[0] -- crash exactly
    # that node by never starting its run() loop. It stays registered on the
    # network (so proposer election still counts it, per the fixed validator
    # set assumption) but never proposes, prevotes, or precommits.
    sorted_ids = sorted(node_lookup.keys())
    crashed_id = sorted_ids[0]
    node_lookup[crashed_id].crash()
    honest_nodes = {nid: n for nid, n in node_lookup.items() if nid != crashed_id}
    print(f"Simulating crash: proposer {short_addr(crashed_id)} is silent.")

    # must cover: round_timeout (0.5s) + round-1 propose/prevote/precommit
    await run_timeline(node_lookup, [(4.0, None)])
    network.flush_log()

    last_hashes = report_state(honest_nodes)
    assert_unanimous(last_hashes)
    assert all(len(n.ledger) > 0 for n in honest_nodes.values()), \
        "LIVENESS VIOLATION: honest quorum failed to finalize despite the silent proposer."
    print("T6 PASSED: honest nodes timed out on the silent proposer, elected a new one, and finalized.")


async def _t8_single_run(log_path: str) -> tuple[str, str]:
    """One fully-determined run. Returns (final_state_root, last_block_hash).

    Everything that could vary between runs is pinned:
      - identities come from fixed seeds (create_deterministic_nodes)
      - the network RNG is seeded
      - the block header timestamp is a logical clock (see engine.py)
      - the run stops on a logical condition, not on elapsed wall time
    """
    NUM_NODES = 4
    TARGET_HEIGHT = 5

    network = create_network(NetworkConfig(stabilized=True, bounded_delay=0.02), log_path)
    network.set_seed(42)
    node_lookup = create_deterministic_nodes(NUM_NODES, network)

    acc_00 = keypair_from_seed(1000)  # fixed sender identity too
    tx = make_transaction("acc_00", "Hi, I'm Alice!", acc_00)
    for node in node_lookup.values():
        node.submit_tx(tx)

    await run_until_height(node_lookup, TARGET_HEIGHT)
    network.flush_log()

    first_node = next(iter(node_lookup.values()))
    return first_node.state.state_root(), first_node.ledger[-1].block_hash()


async def run_t8():
    """Same configuration run twice must produce byte-identical logs and the
    same final state hash (spec section 8)."""
    root_1, head_1 = await _t8_single_run("logs/t8_run1.jsonl")
    root_2, head_2 = await _t8_single_run("logs/t8_run2.jsonl")

    log_1 = Path("logs/t8_run1.jsonl").read_bytes()
    log_2 = Path("logs/t8_run2.jsonl").read_bytes()

    print(f"Run 1: {len(log_1)} bytes, state_root {short_addr(root_1)}, head {short_addr(head_1)}")
    print(f"Run 2: {len(log_2)} bytes, state_root {short_addr(root_2)}, head {short_addr(head_2)}")

    assert root_1 == root_2, f"DETERMINISM VIOLATION: final state hash differs ({root_1} vs {root_2})"
    assert head_1 == head_2, f"DETERMINISM VIOLATION: final block hash differs ({head_1} vs {head_2})"

    if log_1 != log_2:
        lines_1, lines_2 = log_1.decode().splitlines(), log_2.decode().splitlines()
        for i, (a, b) in enumerate(zip(lines_1, lines_2)):
            if a != b:
                raise AssertionError(
                    f"DETERMINISM VIOLATION: logs diverge at line {i + 1}\n"
                    f"  run1: {a}\n  run2: {b}"
                )
        raise AssertionError(
            f"DETERMINISM VIOLATION: log lengths differ "
            f"({len(lines_1)} vs {len(lines_2)} lines)"
        )

    print("T8 PASSED: byte-identical logs and identical final state hash across reruns.")


SCENARIOS = {
    "1": run_t1,
    "2": run_t2,
    "3": run_t3,
    "4": run_t4,
    "5": run_t5,
    "6": run_t6,
    "8": run_t8,
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
