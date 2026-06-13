"""Structured errors — every failure carries enough context to act on.

The platform's debugging history shows that silent or generic failures
("Correlation timed out") cost days; these errors exist so the SDK can
tell users WHAT failed and WHERE, at the call site that caused it.
"""
from __future__ import annotations

from typing import Any, Optional


class NoLagEncodeError(Exception):
    """A payload could not be msgpack-encoded (class instances, cycles...)."""

    def __init__(self, op: str, topic: Optional[str], cause: BaseException) -> None:
        self.op = op
        self.topic = topic
        target = f" to '{topic}'" if topic else ""
        super().__init__(
            f"Cannot encode payload for {op}{target}: {cause}. Payloads must be "
            f"plain msgpack-serializable data (no class instances, coroutines, "
            f"or circular references)."
        )


class NoLagServerError(Exception):
    """A structured error frame sent by the broker."""

    def __init__(self, frame: dict[str, Any]) -> None:
        self.code: Optional[int] = frame.get("code")
        self.error: str = frame.get("error", "unknown_error")
        self.topic: Optional[str] = frame.get("topic")
        self.hint: Optional[str] = frame.get("hint")
        self.msg_ref: Optional[str] = frame.get("msgRef")
        parts = [self.error]
        if self.code:
            parts.append(f"({self.code})")
        if self.topic:
            parts.append(f"on '{self.topic}'")
        if self.hint:
            parts.append(f"— {self.hint}")
        super().__init__(" ".join(parts))
