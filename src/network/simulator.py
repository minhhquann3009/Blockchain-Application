"""
Network simulator.

All inter-node communication goes through Network.send(). It can delay,
drop, duplicate or reorder messages. Every event is logged (append-only,
JSON lines) so a REPORT can point at exact evidence for T1-T8.

This is in-process (asyncio), not real sockets — that matches "simulated
network" in the spec and keeps the lab focused on consensus, not I/O.
"""
import asyncio
import json
import random
import copy
import time
from pathlib import Path
from typing import Literal
from dataclasses import dataclass, field
from ..types.messages import LogMessage, NetworkBody, short_addr

@dataclass
class NetworkConfig:
    min_delay: float = 0.01
    max_delay: float = 0.15
    drop_rate: float = 0.0        # 0.0 - 1.0
    duplicate_rate: float = 0.0
    reorder: bool = True           # if True, delay is randomized per-message (causes reordering)
    stabilized: bool = True        # after stabilization, delay is bounded and no drops
    bounded_delay: float = 0.05    # Delta after stabilization


class Network:
    def __init__(self, config: NetworkConfig, log_path: str|None = None):
        self.config = config
        self.nodes: dict[str, "NodeInbox"] = {}
        self.log_path = Path(log_path) if log_path else None
        self._log_buffer = []
        self.rng = random.Random(1337)  # seeded -> reproducible runs (T8)

    def set_seed(self, seed: int):
        self.rng = random.Random(seed)

    def register(self, node_id: str, inbox: "NodeInbox"):
        self.nodes[node_id] = inbox

    def _log(self, direction: Literal['SENT', 'RECV'], message: LogMessage, message_state: dict|None = None):
        entry_body = {
            'msg': message.log_body.msg_type,
            'direct': direction,
            'from': short_addr(message.log_body.from_node),
            'to': short_addr(message.log_body.to_node),
        }
        if message_state is not None:
            entry_body = entry_body | message_state

        entry = {
            'h': message.height,
            'r': message.round,
            'type': message.log_type,
            'body': entry_body,
        }

        self._log_buffer.append(entry)

    def flush_log(self):        
        if self.log_path is None:
            self._log_buffer.clear()
            return
        self.log_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        with self.log_path.open("w") as f:
            for entry in self._log_buffer:
                f.write(json.dumps(entry, sort_keys=False) + "\n")
        self._log_buffer.clear()

    async def send(
            self,
            height: int, 
            consensus_round: int, 
            log_type: Literal['NETWORK'],
            network_body: NetworkBody
        ):        
        log_message = LogMessage(
            height=height,
            round=consensus_round,
            log_type=log_type,
            log_body=network_body,
        )


        cfg = self.config
        if not cfg.stabilized and self.rng.random() < cfg.drop_rate:
            self._log(direction='SENT', message=log_message, message_state={'sending': 'DROPPED'})
            return

        delay = cfg.bounded_delay if cfg.stabilized else self.rng.uniform(cfg.min_delay, cfg.max_delay)
        self._log(direction='SENT', message=log_message, message_state={'sending': 'SUCCEEDED', 'delay': round(delay, 4)})

        copies = 1
        if not cfg.stabilized and self.rng.random() < cfg.duplicate_rate:
            copies = 2
            self._log(direction='SENT', message=log_message, message_state={'sending': 'DUPLICATED'})

        for _ in range(copies):
            receiver = log_message.log_body.to_node
            consensus_step = network_body.msg_type
            payload = network_body.payload
            asyncio.create_task(self._deliver(receiver, delay, consensus_step, payload, log_message))

    async def _deliver(
            self,
            receiver: str,
            delay: float,
            consensus_step: Literal["PROPOSAL", "PREVOTE", "PRECOMMIT"],
            payload: dict, 
            log_message: LogMessage,
        ):
        await asyncio.sleep(delay)

        inbox = self.nodes.get(receiver)
        if inbox is None:
            return
        self._log(direction='RECV', message=log_message)
        await inbox.queue.put((consensus_step, payload))

    async def broadcast(
            self,
            height: int, 
            round: int, 
            log_type: Literal['NETWORK'],
            log_body: NetworkBody
        ):        
        sender_id = log_body.from_node
        for receiver_id in self.nodes:
            if receiver_id != sender_id:
                log_body.to_node = receiver_id
                await self.send(height, round, log_type, copy.copy(log_body))


class NodeInbox:
    """Each node owns one inbox queue; the node's main loop awaits on it."""
    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue()
