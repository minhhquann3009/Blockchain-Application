import sys, os, asyncio
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.network.simulator import Network, NetworkConfig
from src.types.messages import NetworkBody


class _Recorder:
    """Minimal stand-in for a Node: records what it was handed."""
    def __init__(self):
        self.received = []

    async def handle(self, msg_type, payload):
        self.received.append((msg_type, payload))


def _body(sender, receiver, n):
    return NetworkBody(
        msg_type="PREVOTE",
        from_node=sender,
        to_node=receiver,
        payload={"seq": n},
    )


def _events(network):
    """Sending states recorded in the (unflushed) log buffer."""
    return [
        e.get("body", {}).get("sending")
        for e in network._log_buffer
        if isinstance(e.get("body"), dict)
    ]


def test_sender_within_rate_limit_is_not_blocked():
    async def scenario():
        net = Network(NetworkConfig(stabilized=True, bounded_delay=0.01,
                                    rate_limit=5, rate_window=1.0))
        recv = _Recorder()
        net.register("receiver", recv)
        for i in range(5):
            await net.send(0, 0, "NETWORK", _body("sender", "receiver", i))
        await net.clock.run()
        return net, recv

    net, recv = asyncio.run(scenario())
    assert "BLOCKED" not in _events(net)
    assert len(recv.received) == 5


def test_overactive_peer_is_blocked():
    """Exceeding the limit must suppress the message AND block the peer."""
    async def scenario():
        net = Network(NetworkConfig(stabilized=True, bounded_delay=0.01,
                                    rate_limit=3, rate_window=1.0,
                                    block_duration=1.0))
        recv = _Recorder()
        net.register("receiver", recv)
        for i in range(6):
            await net.send(0, 0, "NETWORK", _body("spammer", "receiver", i))
        await net.clock.run()
        return net, recv

    net, recv = asyncio.run(scenario())
    assert "BLOCKED" in _events(net), "overactive peer was never blocked"
    # only the messages sent before the limit tripped get through
    assert len(recv.received) == 3, f"expected 3 delivered, got {len(recv.received)}"


def test_block_expires_and_peer_is_readmitted():
    """A block is temporary: after block_duration of virtual time the peer
    must be allowed to send again, and the log must show UNBLOCKED."""
    async def scenario():
        net = Network(NetworkConfig(stabilized=True, bounded_delay=0.01,
                                    rate_limit=2, rate_window=0.5,
                                    block_duration=1.0))
        recv = _Recorder()
        net.register("receiver", recv)
        for i in range(4):  # trips the limit, peer gets blocked
            await net.send(0, 0, "NETWORK", _body("spammer", "receiver", i))
        await net.clock.run()

        # advance virtual time past both the rate window and the block
        net.clock.schedule(2.0, _noop)
        await net.clock.run()

        await net.send(0, 0, "NETWORK", _body("spammer", "receiver", 99))
        await net.clock.run()
        return net, recv

    net, recv = asyncio.run(scenario())
    assert "UNBLOCKED" in _events(net), "peer was never readmitted after the block expired"
    assert recv.received[-1][1]["seq"] == 99, "message after unblock was not delivered"


async def _noop():
    return None


if __name__ == "__main__":
    test_sender_within_rate_limit_is_not_blocked()
    test_overactive_peer_is_blocked()
    test_block_expires_and_peer_is_readmitted()
    print("All network tests passed.")
