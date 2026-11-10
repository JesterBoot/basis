from typing import Any

import pytest

from utils.validation import ensure_storable_text, validate_json_tree


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
