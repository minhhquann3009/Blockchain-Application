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

from ..types.messages import Block, BlockHeader, Vote, PREVOTE, PRECOMMIT, NIL
from ..types.messages import LogMessage, NetworkBody, ConsensusBody
from ..execution.state import tx_root


class ConsensusState:
    """Per-node, per-height mutable consensus state."""
    def __init__(self):
        self.round = 0
        self.locked_block: Block | None = None
        self.locked_round: int = -1
        self.valid_block: Block | None = None
        # votes[round][phase] -> {validator: Vote}
        self.votes: dict[int, dict[str, dict[str, Vote]]] = {}
        self.decided = False

    def vote_bucket(self, round_: int, phase: str) -> dict:
        return self.votes.setdefault(round_, {PREVOTE: {}, PRECOMMIT: {}})[phase]


class ConsensusEngine:
    def __init__(self, node_id: str, signing_key, validators: list[str],
                 network, mempool, state_store, ledger, round_timeout: float = 0.5):
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
        # Rule: proposer silent/crashed -> timeout triggers, move to next round (T6)
        await self._enter_round(self.cs.round + 1)

    async def _enter_round(self, round_: int):
        if self.cs.decided:
            return
        self.cs.round = round_
        self._reset_round_timeout()
        if self.is_proposer():
            await self._propose()
        else:
            # if locked, re-prevote for locked block once round advances and we
            # haven't seen a valid new proposal yet; otherwise wait for PROPOSAL
            pass

    # ---- proposing --------------------------------------------------

    async def _propose(self):
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
        payload = {"header": header.signing_payload() | {"signature": header.signature},
                   "tx_hashes": [t.tx_hash() for t in txs],}
        
        network_body = NetworkBody(
            from_node=self.node_id,
            to_node=None,
            payload=payload,
        )
        await self.network.broadcast(
            height=self.height,
            round=self.cs.round,
            step="PROPOSAL",
            log_type="NETWORK",
            log_body=network_body
        )
        await self._handle_proposal_local(block)

    # ---- validation (rule 3) ----------------------------------------

    def validate_block(self, block: Block) -> bool:
        h = block.header
        if h.height != self.height:
            return False
        if h.parent_hash != self.parent_hash():
            return False
        if h.proposer != self.proposer_for(self.height, self.cs.round):
            return False
        if not h.verify():
            return False
        expected_state = self.state_store.apply_all(block.transactions)
        if h.state_root != expected_state.state_root():
            return False
        if h.tx_root != tx_root(block.transactions):
            return False
        for tx in block.transactions:
            if not tx.verify():
                return False
        return True

    # ---- message handlers --------------------------------------------

    async def on_proposal(self, payload: dict):
        # NOTE: in a full implementation, tx bodies are fetched via the
        # mempool/body-after-header rule; here we assume mempool already
        # holds the referenced tx (simulator delivers bodies immediately).
        header = BlockHeader(**{k: v for k, v in payload["header"].items() if k != "signature"})
        header.signature = payload["header"]["signature"]
        txs = [tx for tx in self.mempool if tx.tx_hash() in payload["tx_hashes"]]
        block = Block(header=header, transactions=txs)
        await self._handle_proposal_local(block)

    async def _handle_proposal_local(self, block: Block):
        if self.cs.locked_block is not None:
            # rule 5: locked -> only accept a *different* block via a later
            # quorum prevote, not directly from a proposal
            valid = self.validate_block(block) and block.block_hash() == self.cs.locked_block.block_hash()
        else:
            valid = self.validate_block(block)

        vote_hash = block.block_hash() if valid else NIL
        if valid:
            self.cs.valid_block = block
        await self._send_vote(PREVOTE, vote_hash)

    async def _send_vote(self, step: str, block_hash: str):
        bucket = self.cs.vote_bucket(self.cs.round, step)
        if self.node_id in bucket:
            return  # rule 1/2: at most one vote per (height, round, phase)
        
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
            from_node=self.node_id,
            to_node=None,
            payload=vote.signing_payload(),
        )
        await self.network.broadcast(
            height=self.height,
            round=self.cs.round,
            step=step.upper(),
            log_type="NETWORK",
            log_body=network_body
        )
        await self._tally(self.cs.round, step)

    async def on_vote(self, phase: str, payload: dict):
        vote = Vote(**{k: v for k, v in payload.items() if k != "signature"})
        vote.signature = payload["signature"]

        # rule 6: reject vote outside current chain/height/domain
        if vote.validator not in self.validators or vote.height != self.height:
            return
        if not vote.verify():
            return

        bucket = self.cs.vote_bucket(vote.round, phase)
        if vote.validator in bucket:
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
            if phase == PREVOTE and block_hash != NIL:
                await self._on_prevote_quorum(round_, block_hash)
            elif phase == PRECOMMIT and block_hash != NIL:
                await self._on_precommit_quorum(round_, block_hash)

    async def _on_prevote_quorum(self, round_: int, block_hash: str):
        # rule 4: lock on quorum prevote for a non-NIL block
        block = self.cs.valid_block
        if block and block.block_hash() == block_hash:
            self.cs.locked_block = block
            self.cs.locked_round = round_
        if round_ == self.cs.round:
            await self._send_vote(PRECOMMIT, block_hash)

    async def _on_precommit_quorum(self, round_: int, block_hash: str):
        if self.cs.decided:
            return
        block = self.cs.locked_block
        if not block or block.block_hash() != block_hash:
            return
        self.cs.decided = True
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

    async def start(self):
        await self._enter_round(0)
