import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.crypto.signing import generate_keypair, pubkey_hex
from src.types.messages import Transaction
from src.execution.state import State


def make_tx(sk, pub, name, value, nonce):
    tx = Transaction(sender=pub, key=f"{pub}/{name}", value=value, nonce=nonce)
    tx.sign(sk)
    return tx


def test_same_ordered_txs_give_same_state_root():
    sk, vk = generate_keypair()
    pub = pubkey_hex(vk)
    txs = [make_tx(sk, pub, "message", "hello", 1), make_tx(sk, pub, "counter", "1", 2)]

    s1 = State().apply_all(txs)
    s2 = State().apply_all(txs)  # simulate a second node applying the same list
    assert s1.state_root() == s2.state_root()


def test_replayed_transaction_applied_once():
    sk, vk = generate_keypair()
    pub = pubkey_hex(vk)
    tx = make_tx(sk, pub, "message", "hello", 1)

    s = State()
    assert s.apply(tx) is True
    assert s.apply(tx) is False  # T4: duplicate/replay must not apply twice
    assert s.data[f"{pub}/message"] == "hello"


def test_cannot_write_key_owned_by_someone_else():
    sk_a, vk_a = generate_keypair()
    sk_b, vk_b = generate_keypair()
    pub_a, pub_b = pubkey_hex(vk_a), pubkey_hex(vk_b)

    tx = Transaction(sender=pub_a, key=f"{pub_b}/message", value="hacked", nonce=1)
    tx.sign(sk_a)
    s = State()
    assert s.apply(tx) is False


if __name__ == "__main__":
    test_same_ordered_txs_give_same_state_root()
    test_replayed_transaction_applied_once()
    test_cannot_write_key_owned_by_someone_else()
    print("All execution tests passed.")
