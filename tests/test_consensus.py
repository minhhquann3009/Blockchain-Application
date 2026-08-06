"""
Vote counting and block validation (spec s.9 explicitly names both).

These sit between the pure-crypto tests and the full scenarios: they drive
one ConsensusEngine directly so a single rule can be checked in isolation,
which a whole-network run cannot do -- in a scenario a miscounted vote may
still produce a correct-looking chain, and the bug would go unseen.
"""
import sys, os, asyncio
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.crypto.signing import keypair_from_seed, pubkey_hex
from src.types.messages import (
    Transaction, Block, BlockHeader, Vote, PREVOTE, PRECOMMIT, NIL,
)
from src.network.simulator import Network, NetworkConfig
from src.node import Node
from src.execution.state import tx_root


def _network(num_nodes=4):
    net = Network(NetworkConfig(stabilized=True, bounded_delay=0.02))
    net.set_seed(11)
    accounts = [keypair_from_seed(i) for i in range(num_nodes)]
    ids = sorted(pubkey_hex(vk) for _, vk in accounts)
    nodes = {pubkey_hex(vk): Node(pubkey_hex(vk), sk, ids, net) for sk, vk in accounts}
    return net, nodes, ids, {pubkey_hex(vk): sk for sk, vk in accounts}


def _signed_vote(signing_key, validator, height, round_, phase, block_hash):
    v = Vote(validator=validator, height=height, round=round_,
             phase=phase, block_hash=block_hash)
    v.sign(signing_key)
    return v


def _payload(v):
    return v.signing_payload() | {"signature": v.signature}


# ---------------------------------------------------------------- vote counting

def test_quorum_is_two_f_plus_one():
    """n = 3f+1 -> quorum must be 2f+1, not a simple majority."""
    net, nodes, ids, _ = _network(4)
    eng = nodes[ids[0]].engine
    assert eng.f == 1 and eng.quorum == 3, f"f={eng.f}, quorum={eng.quorum}"

    net7, nodes7, ids7, _ = _network(7)
    eng7 = nodes7[ids7[0]].engine
    assert eng7.f == 2 and eng7.quorum == 5, f"f={eng7.f}, quorum={eng7.quorum}"


def test_duplicate_vote_counted_once():
    """Rule 7: at most one valid vote per validator per (height, round, phase)."""
    async def scenario():
        net, nodes, ids, keys = _network(4)
        eng = nodes[ids[0]].engine
        voter = ids[1]
        v = _signed_vote(keys[voter], voter, 0, 0, PREVOTE, "a" * 64)
        for _ in range(5):  # same vote delivered five times
            await eng.on_vote("prevote", _payload(v))
        return eng

    eng = asyncio.run(scenario())
    bucket = eng.cs.vote_bucket(0, PREVOTE)
    assert len(bucket) == 1, f"duplicate votes were double-counted: {len(bucket)}"


def test_vote_from_unknown_validator_is_ignored():
    """Rule 6: only the fixed validator set may vote."""
    async def scenario():
        net, nodes, ids, keys = _network(4)
        eng = nodes[ids[0]].engine
        outsider_sk, outsider_vk = keypair_from_seed(9999)  # not a validator
        v = _signed_vote(outsider_sk, pubkey_hex(outsider_vk), 0, 0, PREVOTE, "a" * 64)
        await eng.on_vote("prevote", _payload(v))
        return eng

    eng = asyncio.run(scenario())
    assert len(eng.cs.vote_bucket(0, PREVOTE)) == 0, "outsider's vote was counted"


def test_vote_for_wrong_height_is_ignored():
    async def scenario():
        net, nodes, ids, keys = _network(4)
        eng = nodes[ids[0]].engine
        voter = ids[1]
        v = _signed_vote(keys[voter], voter, 99, 0, PREVOTE, "a" * 64)  # wrong height
        await eng.on_vote("prevote", _payload(v))
        return eng

    eng = asyncio.run(scenario())
    assert len(eng.cs.vote_bucket(0, PREVOTE)) == 0, "vote for another height was counted"


def test_vote_with_bad_signature_is_ignored():
    async def scenario():
        net, nodes, ids, keys = _network(4)
        eng = nodes[ids[0]].engine
        voter = ids[1]
        v = _signed_vote(keys[voter], voter, 0, 0, PREVOTE, "a" * 64)
        v.block_hash = "b" * 64  # tamper AFTER signing
        await eng.on_vote("prevote", _payload(v))
        return eng

    eng = asyncio.run(scenario())
    assert len(eng.cs.vote_bucket(0, PREVOTE)) == 0, "tampered vote was counted"


def test_votes_are_bucketed_per_round():
    """Votes for round 0 and round 1 must never be pooled together --
    otherwise two half-quorums in different rounds could look like one."""
    async def scenario():
        net, nodes, ids, keys = _network(4)
        eng = nodes[ids[0]].engine
        for i, round_ in ((1, 0), (2, 1)):
            v = _signed_vote(keys[ids[i]], ids[i], 0, round_, PREVOTE, "a" * 64)
            await eng.on_vote("prevote", _payload(v))
        return eng

    eng = asyncio.run(scenario())
    assert len(eng.cs.vote_bucket(0, PREVOTE)) == 1
    assert len(eng.cs.vote_bucket(1, PREVOTE)) == 1


# ------------------------------------------------------------ block validation

def _valid_block(eng, signing_key, proposer, txs, round_=0):
    state = eng.state_store.apply_all(txs)
    header = BlockHeader(
        height=eng.height,
        parent_hash=eng.parent_hash(),
        proposer=proposer,
        state_root=state.state_root(),
        tx_root=tx_root(txs),
        timestamp=eng._logical_timestamp(round_),
    )
    header.sign(signing_key)
    return Block(header=header, transactions=txs)


def _tx(seed=1000):
    sk, vk = keypair_from_seed(seed)
    sender = pubkey_hex(vk)
    tx = Transaction(sender=sender, key=f"{sender}/acc", value="v", nonce=1)
    tx.sign(sk)
    return tx


def _setup_validator():
    net, nodes, ids, keys = _network(4)
    eng = nodes[ids[1]].engine          # the validator doing the checking
    proposer = eng.proposer_for(0, 0)   # whoever is legitimately proposing
    return eng, proposer, keys[proposer]


def test_valid_block_is_accepted():
    eng, proposer, sk = _setup_validator()
    block = _valid_block(eng, sk, proposer, [_tx()])
    assert eng.validate_block(block, 0) is True


def test_block_with_wrong_parent_is_rejected():
    eng, proposer, sk = _setup_validator()
    block = _valid_block(eng, sk, proposer, [_tx()])
    block.header.parent_hash = "d" * 64
    block.header.sign(sk)  # re-sign so only the parent link is wrong
    assert eng.validate_block(block, 0) is False


def test_block_from_wrong_proposer_is_rejected():
    """Rule 3: the proposer for this (height, round) is fixed by rotation."""
    net, nodes, ids, keys = _network(4)
    eng = nodes[ids[1]].engine
    legitimate = eng.proposer_for(0, 0)
    impostor = next(i for i in ids if i != legitimate)
    block = _valid_block(eng, keys[impostor], impostor, [_tx()])
    assert eng.validate_block(block, 0) is False


def test_block_with_wrong_state_root_is_rejected():
    eng, proposer, sk = _setup_validator()
    block = _valid_block(eng, sk, proposer, [_tx()])
    block.header.state_root = "e" * 64
    block.header.sign(sk)
    assert eng.validate_block(block, 0) is False


def test_block_with_tampered_signature_is_rejected():
    eng, proposer, sk = _setup_validator()
    block = _valid_block(eng, sk, proposer, [_tx()])
    block.header.timestamp += 1  # tamper AFTER signing, do not re-sign
    assert eng.validate_block(block, 0) is False


def test_block_containing_invalid_transaction_is_rejected():
    eng, proposer, sk = _setup_validator()
    bad = _tx()
    bad.value = "tampered after signing"
    block = _valid_block(eng, sk, proposer, [bad])
    assert eng.validate_block(block, 0) is False


if __name__ == "__main__":
    import sys as _s
    mod = _s.modules[__name__]
    names = [n for n in dir(mod) if n.startswith("test_")]
    for n in sorted(names):
        getattr(mod, n)()
    print(f"All {len(names)} consensus tests passed.")
