"""
Byzantine validator (T7).

The spec's fault model lets a Byzantine validator "send conflicting votes"
and "propose invalid blocks", but NOT forge another validator's signature.
So everything this node sends is correctly signed by its OWN key -- that is
precisely what makes equivocation dangerous and worth testing: the messages
are individually valid, and only comparing them reveals the contradiction.

Two attacks are implemented, both partitioning the peer set in half so the
two halves see irreconcilable statements:

  1. Vote equivocation -- signs TWO different votes for the same
     (height, round, phase), violating rules 1 and 2 of section 6.1.
  2. Proposal equivocation -- when elected proposer, builds two blocks that
     differ only in their header timestamp (so both pass validation) and
     sends a different one to each half.

Safety must hold anyway: with n = 3f+1 and at most f Byzantine validators, a
2f+1 quorum for two conflicting blocks at the same height is impossible,
because the two quorums would have to overlap in at least f+1 validators --
which would require an honest node to have double-voted.
"""
from .consensus.engine import ConsensusEngine, _header_payload
from .node import Node
from .types.messages import Block, BlockHeader, Vote, PREVOTE, PRECOMMIT
from .types.messages import NetworkBody, LogConsensus, short_addr
from .execution.state import tx_root


class ByzantineEngine(ConsensusEngine):
    """Consensus engine that deliberately breaks the vote and proposal rules."""

    def _peer_halves(self) -> tuple[list[str], list[str]]:
        """Split the OTHER validators into two disjoint groups. Sorted first
        so the split -- and therefore the whole attack -- is reproducible."""
        peers = sorted(v for v in self.validators if v != self.node_id)
        mid = len(peers) // 2
        return peers[:mid], peers[mid:]

    async def _send_to(self, targets: list[str], msg_type: str, payload: dict):
        for target in targets:
            body = NetworkBody(
                msg_type=msg_type,
                from_node=self.node_id,
                to_node=target,
                payload=payload,
            )
            await self.network.send(self.height, self.cs.round, "NETWORK", body)

    # ---- attack 1: conflicting votes -------------------------------------

    async def _send_vote(self, step: str, block_hash: str):
        """Sign and send TWO contradictory votes for the same (h, r, phase).

        An honest node refuses to vote twice (rule 1/2); this one signs both
        and shows each half of the network only one of them.
        """
        bucket = self.cs.vote_bucket(self.cs.round, step)
        if self.node_id in bucket:
            return

        group_a, group_b = self._peer_halves()

        # The "honest-looking" vote, for whatever we actually believe.
        vote_a = Vote(validator=self.node_id, height=self.height,
                      round=self.cs.round, phase=step, block_hash=block_hash)
        vote_a.sign(self.signing_key)

        # The contradictory one: a different block hash entirely. Fabricated
        # rather than NIL, so the two halves see two *positive* claims.
        conflicting_hash = "b" * 64 if block_hash != "b" * 64 else "c" * 64
        vote_b = Vote(validator=self.node_id, height=self.height,
                      round=self.cs.round, phase=step, block_hash=conflicting_hash)
        vote_b.sign(self.signing_key)

        bucket[self.node_id] = vote_a  # our own view keeps the first one
        self.cs.step = self.cs.step.__class__.PREVOTE if step == PREVOTE else self.cs.step.__class__.PRECOMMIT

        self._log(LogConsensus(
            self.height, self.cs.round, f'BYZANTINE_EQUIVOCATE_{step.upper()}',
            f"Sent {short_addr(block_hash)} to {len(group_a)} peers, "
            f"{short_addr(conflicting_hash)} to {len(group_b)} peers",
            self.node_id,
        ))

        msg_type = "PREVOTE" if step == PREVOTE else "PRECOMMIT"
        await self._send_to(group_a, msg_type,
                            vote_a.signing_payload() | {"signature": vote_a.signature})
        await self._send_to(group_b, msg_type,
                            vote_b.signing_payload() | {"signature": vote_b.signature})

        await self._tally(self.cs.round, step)

    # ---- attack 2: conflicting proposals ---------------------------------

    async def _propose(self):
        """Propose two different blocks for the same height and round."""
        round_ = self.cs.round
        txs = list(self.mempool)
        new_state = self.state_store.apply_all(txs)

        def build(ts_offset: int) -> Block:
            header = BlockHeader(
                height=self.height,
                parent_hash=self.parent_hash(),
                proposer=self.node_id,
                state_root=new_state.state_root(),
                tx_root=tx_root(txs),
                # Only the timestamp differs, so BOTH blocks pass every
                # validation check an honest node performs -- the conflict is
                # invisible to any node that sees just one of them.
                timestamp=self._logical_timestamp(round_) + ts_offset,
            )
            header.sign(self.signing_key)
            return Block(header=header, transactions=txs)

        block_a, block_b = build(0), build(1)
        group_a, group_b = self._peer_halves()

        self._log(LogConsensus(
            self.height, round_, 'BYZANTINE_EQUIVOCATE_PROPOSAL',
            f"Proposed {short_addr(block_a.block_hash())} to {len(group_a)} peers, "
            f"{short_addr(block_b.block_hash())} to {len(group_b)} peers",
            self.node_id,
        ))

        for block, group in ((block_a, group_a), (block_b, group_b)):
            payload = {
                "header": _header_payload(block.header),
                "tx_hashes": [t.tx_hash() for t in block.transactions],
                "round": round_,
            }
            await self._send_to(group, "PROPOSAL", payload)

        # Locally we act on block_a only.
        self.cs.proposals[round_] = block_a
        await self._handle_proposal_local(block_a, round_)


class ByzantineNode(Node):
    """A Node whose consensus engine equivocates. Same external interface as
    Node, so the network and the test harness treat it identically -- which
    is the point: honest nodes cannot tell it apart from the outside."""

    def __init__(self, node_id: str, signing_key, validators: list[str], network):
        super().__init__(node_id, signing_key, validators, network)
        self.engine = ByzantineEngine(
            node_id=node_id,
            signing_key=signing_key,
            validators=validators,
            network=network,
            mempool=self.mempool,
            state_store=self.state,
            ledger=self.ledger,
        )
