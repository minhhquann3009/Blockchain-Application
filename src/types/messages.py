"""
Core message types. Each type defines its own `signing_payload()` — the
exact dict that gets canonical-encoded and signed/verified. Keep these
payloads minimal but complete: every field that must not be tampered with
has to be inside signing_payload(), or a Byzantine node could mutate it
without invalidating the signature.
"""
from dataclasses import dataclass, field
from typing import Optional

from ..crypto.encoding import hash_obj
from ..crypto.signing import CTX_TX, CTX_HEADER, CTX_VOTE, sign, verify


@dataclass
class Transaction:
    sender: str          # pubkey hex of sender
    key: str              # state key being written, must be owned by sender e.g. "Alice/message"
    value: str
    nonce: int             # prevents trivial replay of the exact same tx
    signature: Optional[str] = None

    def signing_payload(self) -> dict:
        return {"sender": self.sender, "key": self.key, "value": self.value, "nonce": self.nonce}

    def sign(self, signing_key):
        self.signature = sign(signing_key, CTX_TX, self.signing_payload())

    def verify(self) -> bool:
        if self.signature is None:
            return False
        if not self.key.startswith(self.sender[:8]) and not self.key.split("/")[0] == self.sender:
            # ownership check is done at execution layer using a name->pubkey
            # registry in real systems; here we keep it simple, see execution.py
            pass
        return verify(self.sender, CTX_TX, self.signing_payload(), self.signature)

    def tx_hash(self) -> str:
        return hash_obj(self.signing_payload())


@dataclass
class BlockHeader:
    height: int
    parent_hash: str
    proposer: str          # pubkey hex of proposer
    state_root: str         # hash of post-execution state
    tx_root: str             # hash of the ordered tx list
    timestamp: float
    signature: Optional[str] = None

    def signing_payload(self) -> dict:
        return {
            "height": self.height,
            "parent_hash": self.parent_hash,
            "proposer": self.proposer,
            "state_root": self.state_root,
            "tx_root": self.tx_root,
            "timestamp": self.timestamp,
        }

    def sign(self, signing_key):
        self.signature = sign(signing_key, CTX_HEADER, self.signing_payload())

    def verify(self) -> bool:
        if self.signature is None:
            return False
        return verify(self.proposer, CTX_HEADER, self.signing_payload(), self.signature)

    def block_hash(self) -> str:
        return hash_obj(self.signing_payload())


@dataclass
class Block:
    header: BlockHeader
    transactions: list  # list[Transaction]

    def block_hash(self) -> str:
        return self.header.block_hash()


PREVOTE = "prevote"
PRECOMMIT = "precommit"
NIL = "NIL"  # a vote for "no block" this round


@dataclass
class Vote:
    validator: str      # pubkey hex
    height: int
    round: int
    phase: str            # PREVOTE | PRECOMMIT
    block_hash: str       # NIL if voting for nothing
    signature: Optional[str] = None

    def signing_payload(self) -> dict:
        return {
            "validator": self.validator,
            "height": self.height,
            "round": self.round,
            "phase": self.phase,
            "block_hash": self.block_hash,
        }

    def sign(self, signing_key):
        self.signature = sign(signing_key, CTX_VOTE, self.signing_payload())

    def verify(self) -> bool:
        if self.signature is None:
            return False
        return verify(self.validator, CTX_VOTE, self.signing_payload(), self.signature)
