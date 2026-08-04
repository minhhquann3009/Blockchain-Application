"""
Deterministic encoding + hashing.

Rule: two nodes must produce IDENTICAL bytes for the same logical object.
We enforce this with canonical JSON: keys sorted, no whitespace, UTF-8,
and every field explicitly typed (ints stay ints, bytes are hex strings).
"""
import hashlib
import json
from typing import Any


def canonical_bytes(obj: Any) -> bytes:
    """Serialize obj to deterministic bytes. obj must be JSON-safe
    (dict/list/str/int/float/bool/None). Use hex strings for raw bytes."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def hash_obj(obj: Any) -> str:
    """Canonical-encode then hash. Returns hex string (safe to put in JSON)."""
    return sha256_hex(canonical_bytes(obj))
