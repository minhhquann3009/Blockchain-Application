"""
Execution layer: deterministic state transition function.

State is a simple key-value map. A transaction may only write a key it
owns (we use the convention "<pubkey_hex>/<name>" -> only that pubkey
may write it, matching the "Alice/message" example in the spec).
"""
from ..crypto.encoding import hash_obj


class State:
    def __init__(self):
        self.data: dict[str, str] = {}
        self.seen_nonces: dict[str, set] = {}  # sender -> set(nonce) for replay protection

    def clone(self) -> "State":
        s = State()
        s.data = dict(self.data)
        s.seen_nonces = {k: set(v) for k, v in self.seen_nonces.items()}
        return s

    def state_root(self) -> str:
        return hash_obj(self.data)

    def can_apply(self, tx) -> bool:
        if not tx.verify():
            return False
        owner = tx.key.split("/", 1)[0]
        if owner != tx.sender:
            return False  # ownership violation
        if tx.nonce in self.seen_nonces.get(tx.sender, set()):
            return False  # replay (T4)
        return True

    def apply(self, tx) -> bool:
        """Apply one tx if valid. Returns True if applied."""
        if not self.can_apply(tx):
            return False
        self.data[tx.key] = tx.value
        self.seen_nonces.setdefault(tx.sender, set()).add(tx.nonce)
        return True

    def apply_all(self, transactions: list) -> "State":
        """Apply an ordered list of tx to a NEW state, deterministically.
        Same input list -> same resulting state_root on every node."""
        new_state = self.clone()
        for tx in transactions:
            new_state.apply(tx)  # invalid tx are silently skipped, not fatal
        return new_state


def tx_root(transactions: list) -> str:
    return hash_obj([tx.tx_hash() for tx in transactions])
