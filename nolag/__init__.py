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
from .api import NoLagApi, NoLagApiError
from .api_types import (
    NoLagApiOptions,
    ListOptions,
    PaginatedResult,
    ApiError,
    App,
    AppCreate,
    AppUpdate,
    Room,
    RoomCreate,
    RoomUpdate,
    Actor,
    ActorWithToken,
    ActorCreate,
    ActorUpdate,
)

__version__ = "2.0.0"
__all__ = [
    # WebSocket Client
    "NoLag",
    "NoLagOptions",
    "ConnectionStatus",
    "ActorType",
    "QoS",
    "SubscribeOptions",
    "EmitOptions",
    "MessageMeta",
    "ActorPresence",
    # REST API Client
    "NoLagApi",
    "NoLagApiError",
    "NoLagApiOptions",
    "ListOptions",
    "PaginatedResult",
    "ApiError",
    "App",
    "AppCreate",
    "AppUpdate",
    "Room",
    "RoomCreate",
    "RoomUpdate",
    "Actor",
    "ActorWithToken",
    "ActorCreate",
    "ActorUpdate",
]
