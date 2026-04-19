"""Run-time counters for passive logging.

* **Unresolved subject** — no ``PortSpec`` for ``(source_node_id, port_id)``
  (nothing under ``[[nodes.ports]]`` for that pair and not an implicit
  standard port). This is “missing type configuration” for that subject.

* **Unlisted node** — ``source_node_id`` is not present in any ``[[nodes]]``
  block (metadata only; decoding may still succeed via implicit standard ports).

* **Deserialize failed** — ``PortSpec`` was found but ``deserialize`` raised or
  returned ``None`` (payload does not match the configured DSDL type).
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any, TypeVar

_H = TypeVar("_H", bound=Any)


def _top_items(counter: Counter[_H], *, limit: int = 12) -> str:
    if not counter:
        return "{}"
    items = counter.most_common(limit)
    inner = ", ".join(f"{k!r}:{v}" for k, v in items)
    if len(counter) > limit:
        inner += f", …(+{len(counter) - limit} more keys)"
    return "{" + inner + "}"


@dataclass
class RunMetrics:
    """Aggregated metrics for one logger run."""

    unresolved_subject: Counter[tuple[int | None, int]] = field(default_factory=Counter)
    unlisted_node: Counter[int] = field(default_factory=Counter)
    deserialize_failed: Counter[str] = field(default_factory=Counter)

    def note_unresolved_subject(self, source_node_id: int | None, port_id: int) -> None:
        self.unresolved_subject[(source_node_id, port_id)] += 1

    def note_unlisted_node(self, node_id: int) -> None:
        self.unlisted_node[node_id] += 1

    def note_deserialize_failed(self, type_str: str) -> None:
        self.deserialize_failed[type_str] += 1

    def summary_lines(self) -> list[str]:
        """Human-readable lines for final log output."""

        u_subj = sum(self.unresolved_subject.values())
        u_node = sum(self.unlisted_node.values())
        d_fail = sum(self.deserialize_failed.values())
        lines = [
            f"missing type mapping (no PortSpec): {u_subj} transfers; by (node_id, port_id): "
            f"{_top_items(self.unresolved_subject)}",
            f"transfers from nodes not in [[nodes]]: {u_node} total; by node_id: "
            f"{_top_items(self.unlisted_node)}",
            f"deserialize failed (known type, bad payload): {d_fail} transfers; by type: "
            f"{_top_items(self.deserialize_failed)}",
        ]
        return lines
