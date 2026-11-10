import hashlib
import json
from typing import Any


def canonical_json(data: Any) -> bytes:
    return json.dumps(
        data, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()


def sha256_hex(data: Any) -> str:
    return hashlib.sha256(canonical_json(data)).hexdigest()
