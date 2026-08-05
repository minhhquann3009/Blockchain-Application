"""
Simplified Tendermint-style BFT consensus.

Implements the 7 rules from Lab spec section 6.1:
  1. sign >=1 PREVOTE per (height, round)
  2. sign >=1 PRECOMMIT per (height, round)
  3. only prevote a block that passes full validation
  4. lock on quorum PREVOTE for a non-NIL block
  5. only unlock via a later-round quorum PREVOTE for a different block
  6. never vote outside the current chain/height/round/phase/domain
  7. ignore duplicate votes, count <=1 vote per validator per (h,r,phase)

n = 3f + 1 validators. Quorum = 2f + 1 (i.e. > 2n/3).
"""
import asyncio
import time
from enum import Enum
from typing import Literal

from ..types.messages import Block, BlockHeader, Vote, PREVOTE, PRECOMMIT, NIL
from ..types.messages import LogMessage, NetworkBody, LogConsensus, short_addr
from ..execution.state import tx_root
from ..network.simulator import Network


class Step(str, Enum):
    PROPOSE = "PROPOSE"
    PREVOTE = "PREVOTE"
    PRECOMMIT = "PRECOMMIT"
    COMMIT = "COMMIT"


def _header_payload(header: BlockHeader) -> dict:
    return header.signing_payload() | {"signature": header.signature}


def _vote_payload(vote: Vote) -> dict:
    return vote.signing_payload() | {"signature": vote.signature}


class ConsensusState:
    """Per-node, per-height mutable consensus state."""
    def __init__(self):
        self.round = 0
        self.step = Step.PROPOSE

        self.locked_block: Block | None = None
        self.locked_round: int = -1
        self.valid_block: Block | None = None
        self.valid_round: int = -1

        # proposals[round] -> Block. Kept PER ROUND -- never overwritten by a
        # later round's proposal. This is what _on_prevote_quorum /
        # _on_precommit_quorum look up, so a quorum for round r is always
        # resolved against round r's actual block, regardless of what round
        # we've personally moved on to.
        self.proposals: dict[int, Block] = {}

        # votes[round][phase] -> {validator: Vote}
        self.votes: dict[int, dict[str, dict[str, Vote]]] = {}

        self.decided = False

    def vote_bucket(self, round_: int, phase: str) -> dict:
        return self.votes.setdefault(round_, {PREVOTE: {}, PRECOMMIT: {}})[phase]


class ConsensusEngine:
    def __init__(self, node_id: str, signing_key, validators: list[str],
                 network: Network, mempool, state_store, ledger, round_timeout: float = 0.5):
        self.node_id = node_id                # our own pubkey hex
        self.signing_key = signing_key
        self.validators = validators           # sorted list of pubkey hex, fixed set
        self.n = len(validators)
        self.f = (self.n - 1) // 3
        self.quorum = 2 * self.f + 1
        self.network = network
        self.mempool = mempool                 # list[Transaction] pending
        self.state_store = state_store          # execution.State, latest finalized state
        self.ledger = ledger                    # list[Block], finalized chain
        self.round_timeout = round_timeout

        self.height = len(ledger)
        self.cs = ConsensusState()
        self._timeout_task: asyncio.Task | None = None

    # ---- logging -------------------------------------------------

    def _log(self, log_message: LogConsensus):
        entry = {
            'h': log_message.height,
            'r': log_message.round,
            'event': log_message.event,
            'msg': log_message.message,
            'node': short_addr(log_message.node_id),
        }
        self.network._log_buffer.append(entry)

    # ---- helpers -------------------------------------------------

    def proposer_for(self, height: int, round_: int) -> str:
        return self.validators[(height + round_) % self.n]

    def is_proposer(self) -> bool:
        return self.proposer_for(self.height, self.cs.round) == self.node_id

    def parent_hash(self) -> str:
        return self.ledger[-1].block_hash() if self.ledger else "GENESIS"

    def _reset_round_timeout(self):
        if self._timeout_task:
            self._timeout_task.cancel()
        self._timeout_task = asyncio.create_task(self._round_timeout())

    async def _round_timeout(self):
        await asyncio.sleep(self.round_timeout)
        log_msg = LogConsensus(self.height, self.cs.round, 'ROUND_TIMEOUT', "Round timeout", self.node_id)
        self._log(log_message=log_msg)
        # Rule: proposer silent/crashed -> timeout triggers, move to next round (T6)
        await self._enter_round(self.cs.round + 1)

    async def _enter_round(self, round_: int):
        if self.cs.decided:
            return
        
        self.cs.round = round_
        self.cs.step = Step.PROPOSE

        self._reset_round_timeout()
        if self.is_proposer():
            await self._propose()
        else:
            # if locked, re-prevote for locked block once round advances and we
            # haven't seen a valid new proposal yet; otherwise wait for PROPOSAL
            pass

    # ---- proposing --------------------------------------------------

    async def _propose(self):
        round_ = self.cs.round

        if self.cs.valid_block is not None:
            # Proof-of-Lock rule: if an earlier round's prevote quorum gave
            # us a valid_block, we MUST re-propose that exact value instead
            # of a fresh one. Skipping this is a liveness bug -- without it,
            # rotating proposers can keep proposing different fresh blocks
            # forever and the network never converges.
            block = self.cs.valid_block
        else:
            txs = list(self.mempool)
            new_state = self.state_store.apply_all(txs)
            header = BlockHeader(
                height=self.height,
                parent_hash=self.parent_hash(),
                proposer=self.node_id,
                state_root=new_state.state_root(),
                tx_root=tx_root(txs),
                timestamp=time.time(),
            )
            header.sign(self.signing_key)
            block = Block(header=header, transactions=txs)

        self.cs.proposals[round_] = block
        payload = {
            "header": _header_payload(block.header),
            "tx_hashes": [t.tx_hash() for t in block.transactions],
            "round": round_,
        }
        
        network_body = NetworkBody(
            msg_type="PROPOSAL",
            from_node=self.node_id,
            to_node=None,
            payload=payload,
        )
        await self.network.broadcast(
            height=self.height,
            round=self.cs.round,
            log_type="NETWORK",
            log_body=network_body
        )
        await self._handle_proposal_local(block, round_)

    # ---- validation (rule 3) ----------------------------------------

    def validate_block(self, block: Block, round_: int) -> bool:
        def _log_invalid_block(event_index: int):
            self._log(LogConsensus(self.height, self.cs.round, f'VAL0{event_index}_BLOCK', f"Invalid block {short_addr(block.block_hash())}", self.node_id))

        h = block.header
        if h.height != self.height:
            _log_invalid_block(0)
            return False
        if h.parent_hash != self.parent_hash():
            _log_invalid_block(1)
            return False
        if h.proposer != self.proposer_for(self.height, round_):
            _log_invalid_block(2)
            return False
        if not h.verify():
            _log_invalid_block(3)
            return False
        expected_state = self.state_store.apply_all(block.transactions)
        if h.state_root != expected_state.state_root():
            _log_invalid_block(4)
            return False
        if h.tx_root != tx_root(block.transactions):
            _log_invalid_block(5)
            return False
        for tx in block.transactions:
            if not tx.verify():
                _log_invalid_block(6)
                return False

        return True

    # ---- message handlers --------------------------------------------

    async def on_proposal(self, payload: dict):
        round_ = payload["round"]

        header_fields = {k: v for k, v in payload["header"].items() if k != "signature"}
        header = BlockHeader(**header_fields)
        header.signature = payload["header"]["signature"]

        # Preserve the PROPOSER's tx order (tx_root is order-sensitive) --
        # do not just filter the local mempool, which may be ordered
        # differently.
        tx_by_hash = {t.tx_hash(): t for t in self.mempool}
        if any(h not in tx_by_hash for h in payload["tx_hashes"]):
            # Body not (yet) available locally (Section 6: header is
            # broadcast before body). A full implementation would request
            # the missing tx bodies here and retry once they arrive.
            return
        txs = [tx_by_hash[h] for h in payload["tx_hashes"]]
        block = Block(header=header, transactions=txs)
        await self._handle_proposal_local(block, round_)

    async def _handle_proposal_local(self, block: Block, round_: int):
        # Keep the block for this round even if we're not ready to act on
        # it yet (e.g. it's for a future round) -- a precommit quorum may
        # reference it later regardless of what round we're personally in.
        self.cs.proposals.setdefault(round_, block)

        # Rule 3 gate: only react (prevote) for the round we're currently
        # in, and only while still waiting for a proposal.
        if round_ != self.cs.round or self.cs.step != Step.PROPOSE:
            return

        structurally_ok = self.validate_block(block, round_)

        if self.cs.locked_block is not None:
            # Rule 5: locked on B -> may only prevote a *different* block
            # after observing a later-round quorum prevote for it (handled
            # in _on_prevote_quorum), never directly off a proposal.
            valid = structurally_ok and block.block_hash() == self.cs.locked_block.block_hash()
        else:
            valid = structurally_ok

        vote_hash = block.block_hash() if valid else NIL
        await self._send_vote(PREVOTE, vote_hash)

    async def _send_vote(self, step: str, block_hash: str):
        bucket = self.cs.vote_bucket(self.cs.round, step)
        if self.node_id in bucket:
            return  # rule 1/2: at most one vote per (height, round, phase)

        self.cs.step = Step.PREVOTE if step == PREVOTE else Step.PRECOMMIT
        
        vote = Vote(
            validator=self.node_id,
            height=self.height,
            round=self.cs.round,
            phase=step,
            block_hash=block_hash
        )
        vote.sign(self.signing_key)
        bucket[self.node_id] = vote

        network_body = NetworkBody(
            msg_type=self.cs.step,
            from_node=self.node_id,
            to_node=None,
            payload=vote.signing_payload() | {"signature": vote.signature},
        )
        await self.network.broadcast(
            height=self.height,
            round=self.cs.round,
            log_type="NETWORK",
            log_body=network_body
        )
        await self._tally(self.cs.round, step)

    async def on_vote(self, phase: str, payload: dict):
        vote = Vote(**{k: v for k, v in payload.items() if k != "signature"})
        vote.signature = payload["signature"]

        # rule 6: reject vote outside current chain/height/domain
        if vote.validator not in self.validators or vote.height != self.height:
            log_msg = f"Rejected validator {short_addr(vote.validator)}, or vote height {vote.height} differs from current height {self.height}!"
            self._log(LogConsensus(self.height, self.cs.round, f'ON_VOTE_{phase.upper()}', log_msg, self.node_id))
            return
        if not vote.verify():
            log_msg = "Invalid vote signature!"
            self._log(LogConsensus(self.height, self.cs.round, f'ON_VOTE_{phase.upper()}', log_msg, self.node_id))
            return

        bucket = self.cs.vote_bucket(vote.round, phase)
        if vote.validator in bucket:
            log_msg = "Rejected duplicated vote!"
            self._log(LogConsensus(self.height, self.cs.round, f'ON_VOTE_{phase.upper()}', log_msg, self.node_id))
            return  # rule 7: ignore duplicate / already-counted vote
        bucket[vote.validator] = vote
        await self._tally(vote.round, phase)

    # ---- quorum logic --------------------------------------------------

    async def _tally(self, round_: int, phase: str):
        bucket = self.cs.vote_bucket(round_, phase)
        if len(bucket) < self.quorum:
            return

        counts: dict[str, int] = {}
        for v in bucket.values():
            counts[v.block_hash] = counts.get(v.block_hash, 0) + 1

        for block_hash, count in counts.items():
            if count < self.quorum:
                continue
            if phase == PREVOTE:
                if block_hash == NIL:
                    await self._on_prevote_nil_quorum(round_)
                else:
                    await self._on_prevote_quorum(round_, block_hash)
            else:
                if block_hash == NIL:
                    await self._on_precommit_nil_quorum(round_)
                else:
                    await self._on_precommit_quorum(round_, block_hash)

    async def _on_prevote_quorum(self, round_: int, block_hash: str):
        block = self.cs.proposals.get(round_)
        if block is None or block.block_hash() != block_hash:
            return  # haven't seen the matching proposal body for this round yet

        self._log(LogConsensus(self.height, self.cs.round, f'PREVOTE_QUORUM', f"Vote PRECOMMIT for block {short_addr(block_hash)}!", self.node_id))

        # Rule 4: lock on quorum prevote for a non-NIL block. Guard against a
        # stale, out-of-order quorum from an OLDER round overriding a lock
        # we've already moved past -- locking must only ever move forward.
        if round_ >= self.cs.locked_round:
            self.cs.locked_block = block
            self.cs.locked_round = round_
        # valid_block/valid_round track the latest Proof-of-Lock independent
        # of our own lock; _propose() reuses this when we become proposer.
        if round_ >= self.cs.valid_round:
            self.cs.valid_block = block
            self.cs.valid_round = round_
        if round_ == self.cs.round and self.cs.step == Step.PREVOTE:
            await self._send_vote(PRECOMMIT, block_hash)

    async def _on_prevote_nil_quorum(self, round_: int):
        # Quorum of nil prevotes: no block can win this round. Move straight
        # to precommit-nil instead of waiting out the full round timeout.
        if round_ == self.cs.round and self.cs.step == Step.PREVOTE:
            await self._send_vote(PRECOMMIT, NIL)

    async def _on_precommit_quorum(self, round_: int, block_hash: str):
        if self.cs.decided:
            return
        block = self.cs.locked_block
        if not block or block.block_hash() != block_hash:
            return
        
        self._log(LogConsensus(self.height, self.cs.round, f'PRECOMMIT_QUORUM', f"Decide and apply block {short_addr(block_hash)}!", self.node_id))

        self.cs.decided = True
        self.cs.step = Step.COMMIT
        if self._timeout_task:
            self._timeout_task.cancel()
        self.ledger.append(block)
        for tx in block.transactions:
            self.state_store.apply(tx)
        self.mempool[:] = [t for t in self.mempool if t not in block.transactions]
        # advance to next height
        self.height += 1
        self.cs = ConsensusState()
        await self._enter_round(0)

    async def _on_precommit_nil_quorum(self, round_: int):
        # Everyone agreed on nothing this round -- advance immediately
        # rather than waiting out the full round_timeout.
        if round_ == self.cs.round and not self.cs.decided:
            await self._enter_round(round_ + 1)

    async def start(self):
        await self._enter_round(0)
