from typing import Any


def ensure_storable_text(text: str, field: str) -> None:
    if "\x00" in text:
        raise ValueError(f"{field} must not contain NUL characters")
    try:
        text.encode()
    except UnicodeEncodeError:
        raise ValueError(f"{field} must be valid UTF-8 text") from None


def validate_json_tree(value: dict[str, Any], max_depth: int, field: str) -> None:
    stack: list[tuple[Any, int]] = [(value, 1)]
    while stack:
        node, depth = stack.pop()
        if isinstance(node, dict | list):
            if depth > max_depth:
                raise ValueError(f"{field} must not be nested deeper than {max_depth}")
            if isinstance(node, dict):
                for key, item in node.items():
                    ensure_storable_text(key, f"{field} keys")
                    stack.append((item, depth + 1))
            else:
                stack.extend((item, depth + 1) for item in node)
        elif isinstance(node, str):
            ensure_storable_text(node, f"{field} values")
