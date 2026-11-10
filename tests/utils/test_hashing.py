import pytest

from utils.hashing import canonical_json, sha256_hex


def test_canonical_json_is_sorted_and_compact() -> None:
    assert canonical_json({"b": 1, "a": {"d": 2, "c": [3, "x"]}}) == (
        b'{"a":{"c":[3,"x"],"d":2},"b":1}'
    )


def test_canonical_json_keeps_unicode_unescaped() -> None:
    assert canonical_json({"k": "ж"}) == '{"k":"ж"}'.encode()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_canonical_json_rejects_non_finite_numbers(value: float) -> None:
    with pytest.raises(ValueError):
        canonical_json({"k": value})


def test_sha256_hex_is_order_independent_and_stable() -> None:
    first = sha256_hex({"a": 1, "b": 2})

    assert first == sha256_hex({"b": 2, "a": 1})
    assert first == "43258cff783fe7036d8a43033f830adfc60ec037382473548ac742b888292777"
