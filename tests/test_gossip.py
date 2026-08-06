"""
Header-before-body gossip (spec s.6).

The scenarios in main.py hand the same transaction to every node's mempool,
so the body-fetch path never fires there. These tests deliberately give the
transaction to the PROPOSER ONLY, forcing the other validators to accept the
header first and then pull the body -- which is the behaviour the spec
prescribes and the only way to know the code path works at all.
"""
import sys, os, asyncio, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.crypto.signing import keypair_from_seed, pubkey_hex
from src.types.messages import Transaction
from src.network.simulator import Network, NetworkConfig
from src.node import Node


def _build(num_nodes=4, log_path=None):
    net = Network(NetworkConfig(stabilized=True, bounded_delay=0.02), log_path=log_path)
    net.set_seed(7)
    accounts = [keypair_from_seed(i) for i in range(num_nodes)]
    ids = sorted(pubkey_hex(vk) for _, vk in accounts)
    nodes = {
        pubkey_hex(vk): Node(pubkey_hex(vk), sk, ids, net)
        for sk, vk in accounts
    }
    return net, nodes, ids


def _make_tx(seed=1000):
    sk, vk = keypair_from_seed(seed)
    sender = pubkey_hex(vk)
    tx = Transaction(sender=sender, key=f"{sender}/acc_00", value="hello", nonce=1)
    tx.sign(sk)
    return tx


def _events(net):
    """Consensus events carry `event` at the top level of the log entry;
    network events nest theirs under `body`. We only care about the former."""
    return [e.get("event") for e in net._log_buffer if e.get("event")]


def test_body_is_fetched_when_only_proposer_has_it():
    async def scenario():
        net, nodes, ids = _build()
        tx = _make_tx()
        # Round 0 proposer is ids[0]; give the tx to nobody else.
        nodes[ids[0]].submit_tx(tx)
        for n in nodes.values():
            await n.start()
        await net.clock.run(until=3.0)
        return net, nodes

    net, nodes = asyncio.run(scenario())
    events = _events(net)
    assert "HEADER_ACCEPTED_BODY_REQUESTED" in events, \
        "no validator requested a body -- header-before-body path never ran"
    assert "BODY_SENT" in events, "proposer never served a requested body"
    assert "BODY_RECEIVED" in events, "no validator absorbed a delivered body"

    # The fetched body must actually let consensus finish.
    assert all(len(n.ledger) > 0 for n in nodes.values()), \
        "nodes fetched bodies but still failed to finalize"


def test_all_nodes_agree_after_body_fetch():
    """Bodies pulled over the wire must produce the same block everywhere --
    a body that arrived by request is not a second-class citizen."""
    async def scenario():
        net, nodes, ids = _build()
        nodes[ids[0]].submit_tx(_make_tx())
        for n in nodes.values():
            await n.start()
        await net.clock.run(until=3.0)
        return nodes

    nodes = asyncio.run(scenario())
    heads = {n.ledger[-1].block_hash() for n in nodes.values() if n.ledger}
    assert len(heads) == 1, f"nodes disagree after body fetch: {heads}"
    states = {n.state.state_root() for n in nodes.values()}
    assert len(states) == 1, f"state roots diverged after body fetch: {states}"


def test_bad_header_is_rejected_without_requesting_body():
    """The whole point of the rule: a header that fails validation must not
    cause anyone to pull a body. We forge a header from a non-proposer."""
    async def scenario():
        net, nodes, ids = _build()
        victim = nodes[ids[1]]
        # ids[2] is not the round-0 proposer, so this header is illegitimate.
        impostor = nodes[ids[2]]
        tx = _make_tx()
        impostor.submit_tx(tx)
        victim.submit_tx(tx)

        from src.types.messages import BlockHeader
        from src.execution.state import tx_root
        header = BlockHeader(
            height=0,
            parent_hash="GENESIS",
            proposer=impostor.node_id,
            state_root=victim.state.apply_all([tx]).state_root(),
            tx_root=tx_root([tx]),
            timestamp=1.0,
        )
        header.sign(impostor.engine.signing_key)
        payload = {
            "header": header.signing_payload() | {"signature": header.signature},
            "tx_hashes": [tx.tx_hash()],
            "round": 0,
        }
        await victim.handle("PROPOSAL", payload)
        return net

    net = asyncio.run(scenario())
    events = _events(net)
    assert "HEADER_REJECTED" in events, "illegitimate header was not rejected"
    assert "HEADER_ACCEPTED_BODY_REQUESTED" not in events, \
        "a body was requested for a header that should have been rejected"


if __name__ == "__main__":
    test_body_is_fetched_when_only_proposer_has_it()
    test_all_nodes_agree_after_body_fetch()
    test_bad_header_is_rejected_without_requesting_body()
    print("All gossip tests passed.")
