"""
NoLag Python SDK
Real-time messaging for Python applications
"""

from .client import NoLag
from .types import (
    NoLagOptions,
    ConnectionStatus,
    ActorType,
    QoS,
    SubscribeOptions,
    EmitOptions,
    MessageMeta,
    ActorPresence,
)

__version__ = "2.0.0"
__all__ = [
    "NoLag",
    "NoLagOptions",
    "ConnectionStatus",
    "ActorType",
    "QoS",
    "SubscribeOptions",
    "EmitOptions",
    "MessageMeta",
    "ActorPresence",
]
