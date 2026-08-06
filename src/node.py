import asyncio

from .consensus.engine import ConsensusEngine
from .execution.state import State


class Node:
    """A validator: crypto identity + mempool + state + ledger + consensus.

    Message handling is driven by the network's VirtualClock rather than by
    a per-node asyncio task. The clock delivers one message at a time, fully
    processing it before the next, which is what makes runs reproducible.
    """

    def __init__(self, node_id: str, signing_key, validators: list[str], network):
        self.node_id = node_id
        self.network = network
        network.register(node_id, self)

        # A crashed node stays in the validator set (proposer election still
        # counts it, per the fixed-validator-set assumption) but neither
        # sends nor receives anything. Used by T6.
        self.crashed: bool = False

        self.mempool: list = []
        self.state = State()
        self.ledger: list = []
        self.engine = ConsensusEngine(
            node_id=node_id,
            signing_key=signing_key,
            validators=validators,
            network=network,
            mempool=self.mempool,
            state_store=self.state,
            ledger=self.ledger,
        )

    def submit_tx(self, tx):
        """Inject a transaction into this node's mempool (client -> node)."""
        self.mempool.append(tx)

    def reset_mempool(self):
        """Reset memory pool for testing"""
        self.mempool.clear()

    def crash(self):
        """Simulate a validator going silent. It stops proposing and voting
        and drops every inbound message from this point on."""
        self.crashed = True

    async def handle(self, msg_type: str, payload: dict):
        if self.crashed:
            return
        if msg_type == "PROPOSAL":
            await self.engine.on_proposal(payload)
        elif msg_type == "PREVOTE":
            await self.engine.on_vote("prevote", payload)
        elif msg_type == "PRECOMMIT":
            await self.engine.on_vote("precommit", payload)
        elif msg_type == "BODY_REQUEST":
            # `sender` is needed so the proposer knows where to send the body.
            await self.engine.on_body_request(payload, payload["_from"])
        elif msg_type == "BODY_RESPONSE":
            await self.engine.on_body_response(payload)

    async def start(self):
        """Kick off consensus at the node's current height."""
        if self.crashed:
            return
        await self.engine.start()
