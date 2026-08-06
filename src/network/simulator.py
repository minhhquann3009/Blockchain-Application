"""
Network simulator.

All inter-node communication goes through Network.send(). It can delay,
drop, duplicate or reorder messages. Every event is logged (append-only,
JSON lines) so a REPORT can point at exact evidence for T1-T8.

This is in-process (asyncio), not real sockets — that matches "simulated
network" in the spec and keeps the lab focused on consensus, not I/O.
"""
import asyncio
import heapq
import json
import random
import copy
import time
from pathlib import Path
from typing import Literal
from dataclasses import dataclass, field
from ..types.messages import LogMessage, NetworkBody, short_addr

class VirtualClock:
    """Deterministic discrete-event scheduler.

    WHY THIS EXISTS. Seeding the RNG, fixing identities and using a logical
    block timestamp is still not enough for byte-identical logs (spec s.8),
    because delivery used `await asyncio.sleep(delay)` -- a REAL-time sleep.
    Two messages scheduled with the same delay are woken by the event loop in
    an order that depends on actual elapsed microseconds, so roughly one run
    in five interleaved them differently. The final state hash stayed correct
    (safety never depended on this), but the log line ORDER drifted.

    The fix is to remove real time from the simulation entirely. Events are
    held in a heap keyed by (virtual_time, insertion_sequence) and executed
    one at a time, each to completion, before the next is popped. The
    insertion sequence breaks ties, so equal-delay messages always resolve in
    the same order. Simulated time only advances when an event fires.

    This is the standard discrete-event simulation approach, and it makes
    determinism a structural property rather than something we hope for.
    """

    def __init__(self):
        self.now: float = 0.0
        self._heap: list = []
        self._seq: int = 0
        self._cancelled: set[int] = set()

    def schedule(self, delay: float, callback) -> int:
        """Queue `callback` (an async zero-arg callable) at now + delay.
        Returns an id usable with cancel()."""
        self._seq += 1
        event_id = self._seq
        # round() keeps float addition from producing tie-break noise
        heapq.heappush(self._heap, (round(self.now + delay, 9), event_id, callback))
        return event_id

    def cancel(self, event_id: int | None) -> None:
        if event_id is not None:
            self._cancelled.add(event_id)

    def reset(self) -> None:
        self.now = 0.0
        self._heap.clear()
        self._seq = 0
        self._cancelled.clear()

    async def run(self, until: float | None = None, stop_condition=None) -> None:
        """Drain the event queue in deterministic order.

        Stops when: the queue empties, virtual time passes `until`, or
        `stop_condition()` returns True (checked between events).
        """
        while self._heap:
            if stop_condition is not None and stop_condition():
                return
            when, event_id, callback = heapq.heappop(self._heap)
            if event_id in self._cancelled:
                self._cancelled.discard(event_id)
                continue
            if until is not None and when > until:
                # put it back so a later run() can continue from here
                heapq.heappush(self._heap, (when, event_id, callback))
                self.now = until
                return
            self.now = when
            await callback()


@dataclass
class NetworkConfig:
    min_delay: float = 0.01
    max_delay: float = 0.15
    drop_rate: float = 0.0        # 0.0 - 1.0

    duplicate_rate: float = 0.0

    reorder_list: list[float] = field(default_factory=list)

    stabilized: bool = True        # after stabilization, delay is bounded and no drops
    bounded_delay: float = 0.05    # Delta after stabilization

    # Outbound rate limiting (spec s.6: "Each node limits outbound rate and
    # may temporarily block overactive peers"). A sender may emit at most
    # `rate_limit` messages per `rate_window` of virtual time; beyond that
    # the receiver blocks it for `block_duration`, dropping its messages.
    # rate_limit = 0 disables the mechanism entirely (default, so existing
    # scenarios keep their current behaviour).
    rate_limit: int = 0
    rate_window: float = 1.0
    block_duration: float = 1.0


class Network:
    def __init__(self, config: NetworkConfig, log_path: str|None = None):
        self.config = config
        self.clock = VirtualClock()
        self.nodes: dict[str, "Node"] = {}  # node_id -> Node (see src/node.py)
        self.log_path = Path(log_path) if log_path else None
        self._log_buffer = []

        self._order_index = 0  # Start from first delay to last delay and add message to _log_reorder
        self._log_reorder = []  # List of (delay, message) sorted by delay to show the order of received messages

        self.rng = random.Random(1337)  # seeded -> reproducible runs (T8)

        # Rate limiting bookkeeping, all keyed on VIRTUAL time so the
        # behaviour is reproducible run to run.
        self._send_times: dict[str, list[float]] = {}   # sender -> send timestamps
        self._blocked_until: dict[tuple[str, str], float] = {}  # (receiver, sender) -> until

    def set_seed(self, seed: int):
        self.rng = random.Random(seed)

    def register(self, node_id: str, node) -> None:
        """Register a Node so the clock can dispatch messages straight to it.
        Since delivery is driven by VirtualClock rather than per-node tasks,
        the network holds the Node itself, not a queue."""
        self.nodes[node_id] = node

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

    def _rate_limit_blocks(self, network_body, log_message) -> bool:
        """Return True if this message must be suppressed.

        Two effects, both required by spec s.6:
          - the sender is throttled once it exceeds `rate_limit` messages
            within `rate_window`;
          - the receiver then BLOCKS that peer for `block_duration`, dropping
            everything from it until the block expires (logged as BLOCK /
            UNBLOCK events so the log shows the peer being penalised and
            later readmitted).
        """
        cfg = self.config
        now = self.clock.now
        sender = network_body.from_node
        receiver = network_body.to_node
        pair = (receiver, sender)

        blocked_until = self._blocked_until.get(pair)
        if blocked_until is not None:
            if now < blocked_until:
                self._log(direction='SENT', message=log_message,
                          message_state={'sending': 'BLOCKED'})
                return True
            del self._blocked_until[pair]
            self._log(direction='SENT', message=log_message,
                      message_state={'sending': 'UNBLOCKED'})

        window_start = now - cfg.rate_window
        times = [t for t in self._send_times.get(sender, []) if t >= window_start]
        times.append(now)
        self._send_times[sender] = times

        if len(times) > cfg.rate_limit:
            self._blocked_until[pair] = now + cfg.block_duration
            self._log(direction='SENT', message=log_message,
                      message_state={'sending': 'BLOCKED',
                                     'reason': 'RATE_LIMIT_EXCEEDED',
                                     'blocked_until': round(now + cfg.block_duration, 4)})
            return True
        return False

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

        if cfg.rate_limit > 0 and self._rate_limit_blocks(network_body, log_message):
            return

        if not cfg.stabilized and self.rng.random() < cfg.drop_rate:
            self._log(direction='SENT', message=log_message, message_state={'sending': 'DROPPED'})
            return

        delay = cfg.bounded_delay if cfg.stabilized else self.rng.uniform(cfg.min_delay, cfg.max_delay)
        if self._order_index < len(cfg.reorder_list):
            delay = cfg.reorder_list[self._order_index]
            self._order_index += 1
        self._log(direction='SENT', message=log_message, message_state={'sending': 'SUCCEEDED', 'delay': round(delay, 4)})

        copies = 1
        if self.rng.random() < cfg.duplicate_rate:
            copies = 2
            self._log(direction='SENT', message=log_message, message_state={'sending': 'DUPLICATED'})


        for _ in range(copies):
            receiver = log_message.log_body.to_node
            consensus_step = network_body.msg_type
            payload = network_body.payload
            # Deterministic: queued on the virtual clock, not a real-time task.
            # Equal delays are tie-broken by insertion order, so delivery
            # sequence is fully reproducible.
            self.clock.schedule(
                delay,
                lambda r=receiver, s=consensus_step, p=payload, m=log_message:
                    self._deliver(r, s, p, m),
            )

    async def _deliver(
            self,
            receiver: str,
            consensus_step: Literal["PROPOSAL", "PREVOTE", "PRECOMMIT"],
            payload: dict,
            log_message: LogMessage,
        ):
        node = self.nodes.get(receiver)
        if node is None or getattr(node, "crashed", False):
            # An unregistered or crashed node silently drops the message --
            # exactly what T6 needs to model a validator that is not running.
            return
        self._log(direction='RECV', message=log_message)
        await node.handle(consensus_step, payload)

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
