import ipaddress
from typing import Any

import pytest

from utils.hashing import canonical_json, sha256_hex
from utils.network import is_private_address, is_private_host
from utils.validation import ensure_storable_text, validate_json_tree


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.1.2.3",
        "172.16.0.1",
        "192.168.0.1",
        "169.254.169.254",
        "100.64.0.1",
        "0.0.0.0",
        "255.255.255.255",
        "224.0.0.1",
        "::1",
        "::",
        "fe80::1",
        "fc00::1",
        "ff02::1",
        "::ffff:127.0.0.1",
        "::ffff:10.0.0.1",
        "2002:7f00:1::",
        "64:ff9b::7f00:1",
        "64:ff9b::a00:1",
    ],
)
def test_non_public_addresses_are_private(address: str) -> None:
    assert is_private_address(ipaddress.ip_address(address)) is True


@pytest.mark.parametrize(
    "address",
    [
        "8.8.8.8",
        "1.1.1.1",
        "2606:4700:4700::1111",
        "::ffff:8.8.8.8",
        "64:ff9b::808:808",
    ],
)
def test_public_addresses_are_not_private(address: str) -> None:
    assert is_private_address(ipaddress.ip_address(address)) is False


@pytest.mark.parametrize(
    "host",
    ["localhost", "LOCALHOST", "localhost.", "api.localhost", "[::1]", "127.0.0.1"],
)
def test_private_hosts(host: str) -> None:
    assert is_private_host(host) is True


@pytest.mark.parametrize(
    "host", ["merchant.example", "webhook-sink", "localhost.example.com", "8.8.8.8"]
)
def test_public_or_unresolved_hosts(host: str) -> None:
    assert is_private_host(host) is False


@pytest.mark.parametrize("text", ["", "plain", "Заказ №42 🍕", "tab\tand\nnewline"])
def test_storable_text_is_accepted(text: str) -> None:
    ensure_storable_text(text, "field")


@pytest.mark.parametrize(
    ("text", "message"),
    [("nul\x00inside", "NUL characters"), ("lone \ud800 surrogate", "valid UTF-8")],
)
def test_unstorable_text_is_rejected(text: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        ensure_storable_text(text, "field")


def nested_dicts(depth: int) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for _ in range(depth - 1):
        value = {"a": value}
    return value


def nested_lists(depth: int) -> dict[str, Any]:
    value: list[Any] = []
    for _ in range(depth - 2):
        value = [value]
    return {"items": value}


@pytest.mark.parametrize(
    "builder", [nested_dicts, nested_lists], ids=["dicts", "lists"]
)
def test_json_tree_depth_limit_is_inclusive(builder: Any) -> None:
    validate_json_tree(builder(5), max_depth=5, field="metadata")

    with pytest.raises(ValueError, match="metadata must not be nested deeper than 5"):
        validate_json_tree(builder(6), max_depth=5, field="metadata")


def test_json_tree_depth_check_does_not_recurse() -> None:
    validate_json_tree(nested_dicts(5000), max_depth=10_000, field="metadata")


@pytest.mark.parametrize(
    ("tree", "message"),
    [
        ({"k\x00": 1}, "metadata keys must not contain NUL"),
        ({"k": "v\x00"}, "metadata values must not contain NUL"),
        ({"k": ["ok", "bad\x00"]}, "metadata values must not contain NUL"),
        ({"k": {"deep": "\ud800"}}, "metadata values must be valid UTF-8"),
    ],
)
def test_json_tree_checks_keys_and_values_everywhere(
    tree: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        validate_json_tree(tree, max_depth=10, field="metadata")


def test_json_tree_ignores_non_string_scalars() -> None:
    validate_json_tree(
        {"n": 1, "f": 1.5, "b": True, "none": None, "list": [1, None, False]},
        max_depth=3,
        field="metadata",
    )


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
