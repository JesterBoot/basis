from tests.support.webhooks import WEBHOOK_SECRET
from utils.signing import sign, verify


def test_signature_round_trip() -> None:
    body = b'{"a":1}'
    signature = sign(WEBHOOK_SECRET, body)

    assert signature.startswith("sha256=")
    assert verify(WEBHOOK_SECRET, body, signature) is True
    assert verify(WEBHOOK_SECRET, body + b" ", signature) is False
    assert verify("other-secret", body, signature) is False
