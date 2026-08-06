"""
Identity + signatures.

Uses Ed25519 (via PyNaCl). Every signed message is domain-separated:
we prefix the canonical bytes with a context string so a signature valid
for a VOTE can never be replayed as a valid HEADER or TX signature.
"""
from nacl.signing import SigningKey, VerifyKey
from nacl.exceptions import BadSignatureError

from .encoding import canonical_bytes, sha256

CHAIN_ID = "lab01-devnet"

CTX_TX = f"TX:{CHAIN_ID}".encode()
CTX_HEADER = f"HEADER:{CHAIN_ID}".encode()
CTX_VOTE = f"VOTE:{CHAIN_ID}".encode()


def generate_keypair():
    """Returns (signing_key, verify_key). signing_key is secret.

    Uses the OS CSPRNG -- keys differ on every run. Use this for anything
    resembling real operation."""
    sk = SigningKey.generate()
    return sk, sk.verify_key


def keypair_from_seed(seed: int):
    """Deterministically derive a keypair from an integer seed.

    FOR TESTING/REPRODUCIBILITY ONLY. The lab spec (section 8) requires that
    re-running the same configuration yields byte-identical logs; that is
    impossible while every run mints fresh random identities, since node_ids
    (and therefore proposer order, log contents, and every signature) change
    each time.

    This is NOT cryptographically secure key generation -- the spec is
    explicit that "random seeds in the simulator do not supply cryptographic
    randomness". A real deployment must use generate_keypair()."""
    seed_bytes = sha256(f"lab01-validator-seed:{seed}".encode())
    sk = SigningKey(seed_bytes)
    return sk, sk.verify_key


def pubkey_hex(verify_key: VerifyKey) -> str:
    return verify_key.encode().hex()


def sign(signing_key: SigningKey, context: bytes, payload: dict) -> str:
    """Sign canonical_bytes(payload) prefixed with a domain context.
    Returns hex signature."""
    msg = context + canonical_bytes(payload)
    sig = signing_key.sign(msg).signature
    return sig.hex()


def verify(verify_key_hex: str, context: bytes, payload: dict, signature_hex: str) -> bool:
    try:
        vk = VerifyKey(bytes.fromhex(verify_key_hex))
        msg = context + canonical_bytes(payload)
        vk.verify(msg, bytes.fromhex(signature_hex))
        return True
    except (BadSignatureError, ValueError, Exception):
        return False
