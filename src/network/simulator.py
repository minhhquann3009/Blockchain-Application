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
import time
from dataclasses import dataclass, field


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
    def __init__(self, config: NetworkConfig, log_path: str = None):
        self.config = config
        self.nodes: dict[str, "NodeInbox"] = {}
        self.log_path = log_path
        self._log_buffer = []
        self.rng = random.Random(1337)  # seeded -> reproducible runs (T8)

    def set_seed(self, seed: int):
        self.rng = random.Random(seed)

    def register(self, node_id: str, inbox: "NodeInbox"):
        self.nodes[node_id] = inbox

    def _log(self, event_type: str, node_id: str, height: int, extra: dict = None):
        entry = {
            "ts": round(time.time(), 6),
            "node": node_id,
            "type": event_type,
            "height": height,
        }
        if extra:
            entry.update(extra)
        self._log_buffer.append(entry)

    def flush_log(self):
        if not self.log_path:
            return
        with open(self.log_path, "a") as f:
            for entry in self._log_buffer:
                f.write(json.dumps(entry, sort_keys=True) + "\n")
        self._log_buffer.clear()

    async def send(self, sender: str, receiver: str, msg_type: str, payload: dict, height: int = -1):
        cfg = self.config
        if not cfg.stabilized and self.rng.random() < cfg.drop_rate:
            self._log("drop", sender, height, {"to": receiver, "msg_type": msg_type})
            return

        delay = cfg.bounded_delay if cfg.stabilized else self.rng.uniform(cfg.min_delay, cfg.max_delay)
        self._log("send", sender, height, {"to": receiver, "msg_type": msg_type, "delay": round(delay, 4)})

        copies = 1
        if not cfg.stabilized and self.rng.random() < cfg.duplicate_rate:
            copies = 2
            self._log("duplicate", sender, height, {"to": receiver, "msg_type": msg_type})

        for _ in range(copies):
            asyncio.create_task(self._deliver(sender, receiver, msg_type, payload, height, delay))

    async def _deliver(self, sender, receiver, msg_type, payload, height, delay):
        await asyncio.sleep(delay)
        inbox = self.nodes.get(receiver)
        if inbox is None:
            return
        self._log("deliver", receiver, height, {"from": sender, "msg_type": msg_type})
        await inbox.queue.put((msg_type, payload))

    async def broadcast(self, sender: str, msg_type: str, payload: dict, height: int = -1):
        for node_id in self.nodes:
            if node_id != sender:
                await self.send(sender, node_id, msg_type, payload, height)


class NodeInbox:
    """Each node owns one inbox queue; the node's main loop awaits on it."""
    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue()
