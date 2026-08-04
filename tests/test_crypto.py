import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.crypto.signing import generate_keypair, pubkey_hex, sign, verify, CTX_TX, CTX_VOTE
from src.types.messages import Transaction


def test_valid_signature_verifies():
    sk, vk = generate_keypair()
    pub = pubkey_hex(vk)
    tx = Transaction(sender=pub, key=f"{pub}/message", value="hi", nonce=1)
    tx.sign(sk)
    assert tx.verify() is True


def test_tampered_payload_fails():
    sk, vk = generate_keypair()
    pub = pubkey_hex(vk)
    tx = Transaction(sender=pub, key=f"{pub}/message", value="hi", nonce=1)
    tx.sign(sk)
    tx.value = "tampered"  # mutate after signing
    assert tx.verify() is False


def test_domain_separation_blocks_cross_context_replay():
    """A signature made for VOTE context must not validate under TX context."""
    sk, vk = generate_keypair()
    pub = pubkey_hex(vk)
    payload = {"height": 1, "round": 0, "phase": "prevote", "block_hash": "abc", "validator": pub}
    sig = sign(sk, CTX_VOTE, payload)
    assert verify(pub, CTX_VOTE, payload, sig) is True
    assert verify(pub, CTX_TX, payload, sig) is False  # T3: wrong domain must be rejected


def test_wrong_signer_fails():
    sk1, vk1 = generate_keypair()
    sk2, vk2 = generate_keypair()
    pub1 = pubkey_hex(vk1)
    tx = Transaction(sender=pub1, key=f"{pub1}/message", value="hi", nonce=1)
    tx.sign(sk2)  # signed by the wrong key
    assert tx.verify() is False


if __name__ == "__main__":
    test_valid_signature_verifies()
    test_tampered_payload_fails()
    test_domain_separation_blocks_cross_context_replay()
    test_wrong_signer_fails()
    print("All crypto tests passed.")
