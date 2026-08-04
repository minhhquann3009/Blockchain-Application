import asyncio

from .network.simulator import NodeInbox
from .consensus.engine import ConsensusEngine
from .execution.state import State


class Node:
    def __init__(self, node_id: str, signing_key, validators: list[str], network):
        self.node_id = node_id
        self.network = network
        self.inbox = NodeInbox()
        network.register(node_id, self.inbox)

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
        self._task: asyncio.Task | None = None

    def submit_tx(self, tx):
        """Inject a transaction into this node's mempool (simulates client -> node)."""
        self.mempool.append(tx)

    async def run(self):
        self._task = asyncio.create_task(self.engine.start())
        while True:
            msg_type, payload = await self.inbox.queue.get()
            if msg_type == "PROPOSAL":
                await self.engine.on_proposal(payload)
            elif msg_type == "PREVOTE":
                await self.engine.on_vote("prevote", payload)
            elif msg_type == "PRECOMMIT":
                await self.engine.on_vote("precommit", payload)

    def stop(self):
        if self._task:
            self._task.cancel()
