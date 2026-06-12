"""
NoLag SDK Types
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class ConnectionStatus(str, Enum):
    """Connection status"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"


class ActorType(str, Enum):
    """Actor types (must mirror the broker / js-sdk list)"""
    DEVICE = "device"
    USER = "user"
    SERVER = "server"
    SERVICE = "service"
    SESSION = "session"
    AGENT = "agent"
    ORCHESTRATOR = "orchestrator"
    OBSERVER = "observer"


class QoS(int, Enum):
    """QoS levels for MQTT"""
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2


@dataclass
class NoLagOptions:
    """Connection options"""
    url: str = "wss://broker.nolag.app/ws"
    reconnect: bool = True
    reconnect_interval: float = 5.0  # seconds
    max_reconnect_attempts: int = 10
    heartbeat_interval: float = 30.0  # seconds, 0 to disable
    disconnect_on_hidden: bool = False
    debug: bool = False
    qos: QoS = QoS.AT_LEAST_ONCE
    load_balance: bool = False
    load_balance_group: Optional[str] = None
    actor_token_id: Optional[str] = None


@dataclass
class SubscribeOptions:
    """Subscribe options"""
    qos: Optional[QoS] = None
    load_balance: Optional[bool] = None
    load_balance_group: Optional[str] = None
    filters: Optional[list[str | list[str]]] = None


@dataclass
class EmitOptions:
    """Emit/publish options"""
    qos: Optional[QoS] = None
    retain: bool = False
    echo: bool = True  # Whether to receive this message back if subscribed
    filter: Optional[str] = None
    filters: Optional[list[str]] = None  # AND composite publish


@dataclass
class MessageMeta:
    """Message metadata"""
    sender: Optional[str] = None
    timestamp: Optional[float] = None
    is_replay: bool = False  # Whether this message is being replayed from history
    msg_id: Optional[str] = None  # Unique message ID for ACK
    filter: Optional[str] = None  # Filter value this message was published with


@dataclass
class ActorPresence:
    """Actor presence info"""
    actor_token_id: str
    actor_type: ActorType
    presence: dict[str, Any] = field(default_factory=dict)
    joined_at: Optional[float] = None


# Lobby types
LobbyPresenceState = dict[str, dict[str, dict[str, Any]]]


@dataclass
class LobbyPresenceEvent:
    """Lobby presence event info"""
    lobby_id: str
    room_id: str
    actor_id: str
    data: dict[str, Any] = field(default_factory=dict)


LobbyPresenceHandler = Callable[["LobbyPresenceEvent"], None]

# Type aliases for callbacks
MessageHandler = Callable[[Any, MessageMeta], None]
EventHandler = Callable[..., None]
AckCallback = Callable[[Optional[Exception]], None]
