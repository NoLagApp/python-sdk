"""
NoLag Python SDK
Real-time messaging for Python applications
"""

from .errors import NoLagEncodeError, NoLagServerError
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
    LobbyPresenceEvent,
    LobbyPresenceState,
    LobbyPresenceHandler,
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
    Scope,
    ScopeCreate,
    ScopeUpdate,
)

# WebRTC support (optional, requires aiortc)
try:
    from .webrtc import WebRTCManager, WebRTCOptions, is_webrtc_available
    _WEBRTC_AVAILABLE = True
except ImportError:
    _WEBRTC_AVAILABLE = False
    WebRTCManager = None  # type: ignore[misc,assignment]
    WebRTCOptions = None  # type: ignore[misc,assignment]

    def is_webrtc_available() -> bool:  # type: ignore[no-redef]
        return False

__version__ = "2.4.0"
__all__ = [
    "NoLagEncodeError",
    "NoLagServerError",
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
    "LobbyPresenceEvent",
    "LobbyPresenceState",
    "LobbyPresenceHandler",
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
    "Scope",
    "ScopeCreate",
    "ScopeUpdate",
    # WebRTC (optional)
    "WebRTCManager",
    "WebRTCOptions",
    "is_webrtc_available",
]
