"""Flatten nested dicts / collections into flat `key.sub[i]...` form.

Ported from ``pyric._util.flatten`` so the Influx field layout is identical.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import Any


def flatten(d: Mapping[Any, Any] | Collection[Any], parent_key: str = "", sep: str = ".") -> dict[str, Any]:
    def add_item(items: list[tuple[str, Any]], new_key: str, v: Any) -> None:
        if isinstance(v, Mapping) or (isinstance(v, Collection) and not isinstance(v, str)):
            items.extend(flatten(v, new_key, sep).items())
        else:
            items.append((new_key, v))

    if isinstance(d, Mapping):
        items: list[tuple[str, Any]] = []
        if len(d) == 1:
            ((k, v),) = d.items()
            add_item(items, parent_key or str(k), v)
        else:
            for k, v in d.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else str(k)
                add_item(items, new_key, v)
        return dict(items)

    if isinstance(d, Collection) and not isinstance(d, str):
        items = []
        for i, v in enumerate(d):
            new_key = f"{parent_key}[{i}]" if parent_key else f"[{i}]"
            add_item(items, new_key, v)
        return dict(items)

    return {}
