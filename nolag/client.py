"""
NoLag Client
WebSocket client for Kraken Proxy with automatic reconnection
"""

import asyncio
import logging
from typing import Any, Callable, Optional

import msgpack
import websockets
from websockets.client import WebSocketClientProtocol

from .types import (
    ActorPresence,
    ActorType,
    AckCallback,
    ConnectionStatus,
    EmitOptions,
    MessageHandler,
    MessageMeta,
    NoLagOptions,
    QoS,
    SubscribeOptions,
)

logger = logging.getLogger("nolag")


class Room:
    """
    Room - Scoped context for pub/sub within an app.room

    Example:
        room = client.set_app('chat').set_room('general')
        await room.subscribe('messages')
        room.on('messages', lambda data, meta: print(data))
        await room.emit('messages', {'text': 'Hello!'})
    """

    def __init__(self, client: "NoLag", app_name: str, room_name: str):
        self._client = client
        self._app_name = app_name
        self._room_name = room_name

    @property
    def prefix(self) -> str:
        """The full topic prefix (app.room)"""
        return f"{self._app_name}.{self._room_name}"

    def _full_topic(self, topic: str) -> str:
        return f"{self.prefix}.{topic}"

    async def subscribe(
        self,
        topic: str,
        options: Optional[SubscribeOptions] = None,
        callback: Optional[AckCallback] = None,
    ) -> None:
        """Subscribe to a topic in this room"""
        await self._client.subscribe(self._full_topic(topic), options, callback)

    async def unsubscribe(
        self,
        topic: str,
        callback: Optional[AckCallback] = None,
    ) -> None:
        """Unsubscribe from a topic in this room"""
        await self._client.unsubscribe(self._full_topic(topic), callback)

    async def emit(
        self,
        topic: str,
        data: Any,
        options: Optional[EmitOptions] = None,
        callback: Optional[AckCallback] = None,
    ) -> None:
        """Emit/publish to a topic in this room"""
        await self._client.emit(self._full_topic(topic), data, options, callback)

    def on(self, topic: str, handler: MessageHandler) -> "Room":
        """Listen for messages on a topic in this room"""
        self._client.on(self._full_topic(topic), handler)
        return self

    def off(self, topic: str, handler: Optional[MessageHandler] = None) -> "Room":
        """Remove message handler for a topic in this room"""
        self._client.off(self._full_topic(topic), handler)
        return self


class App:
    """App context for fluent API"""

    def __init__(self, client: "NoLag", app_name: str):
        self._client = client
        self._app_name = app_name

    def set_room(self, room_name: str) -> Room:
        """Set the room within this app"""
        return Room(self._client, self._app_name, room_name)


class NoLag:
    """
    NoLag Client

    A Socket.IO-style API for real-time messaging via Kraken Proxy.
    Subscriptions are automatically restored on reconnect by the server.

    Example:
        client = NoLag('your_access_token')
        await client.connect()

        # Fluent API (recommended)
        room = client.set_app('chat').set_room('general')
        await room.subscribe('messages')
        room.on('messages', lambda data, meta: print(data))
        await room.emit('messages', {'text': 'Hello!'})

        # Direct API (full topic paths)
        await client.subscribe('chat.general.messages')
        client.on('chat.general.messages', lambda data, meta: print(data))
        await client.emit('chat.general.messages', {'text': 'Hello!'})
    """

    def __init__(self, token: str, options: Optional[NoLagOptions] = None):
        self._token = token
        self._options = options or NoLagOptions()

        self._ws: Optional[WebSocketClientProtocol] = None
        self._status = ConnectionStatus.DISCONNECTED
        self._reconnect_attempts = 0
        self._reconnect_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._receive_task: Optional[asyncio.Task] = None

        # Actor info (populated after auth)
        self._actor_token_id: Optional[str] = None
        self._project_id: Optional[str] = None
        self._actor_type: Optional[ActorType] = None

        # Presence
        self._presence: Optional[dict[str, Any]] = None
        self._presence_map: dict[str, ActorPresence] = {}

        # Event handlers
        self._event_handlers: dict[str, set[Callable]] = {}
        self._message_handlers: dict[str, set[MessageHandler]] = {}
        self._any_handlers: set[Callable[[str, Any, MessageMeta], None]] = set()

        # Auth promise
        self._auth_future: Optional[asyncio.Future] = None

    # ============ Properties ============

    @property
    def connected(self) -> bool:
        """Whether the client is connected"""
        return self._status == ConnectionStatus.CONNECTED

    @property
    def status(self) -> ConnectionStatus:
        """Current connection status"""
        return self._status

    @property
    def actor_id(self) -> Optional[str]:
        """Actor token ID (available after connect)"""
        return self._actor_token_id

    @property
    def project_id(self) -> Optional[str]:
        """Project ID (available after connect)"""
        return self._project_id

    @property
    def actor_type(self) -> Optional[ActorType]:
        """Actor type (available after connect)"""
        return self._actor_type

    @property
    def load_balanced(self) -> bool:
        """Whether load balancing is enabled"""
        return self._options.load_balance

    @property
    def load_balance_group(self) -> Optional[str]:
        """Load balance group name"""
        return self._options.load_balance_group

    # ============ Connection ============

    async def connect(self) -> None:
        """Connect to NoLag"""
        if self._status == ConnectionStatus.CONNECTED:
            return

        if self._status == ConnectionStatus.CONNECTING:
            # Wait for existing connection attempt
            while self._status == ConnectionStatus.CONNECTING:
                await asyncio.sleep(0.1)
            if self._status == ConnectionStatus.CONNECTED:
                return
            raise Exception("Connection failed")

        self._status = ConnectionStatus.CONNECTING
        self._log(f"Connecting to {self._options.url}")

        try:
            self._ws = await websockets.connect(self._options.url)
            self._log("WebSocket opened, authenticating...")

            # Start receive loop
            self._receive_task = asyncio.create_task(self._receive_loop())

            # Authenticate
            restored = await self._authenticate()

            self._status = ConnectionStatus.CONNECTED
            self._reconnect_attempts = 0
            self._log("Connected and authenticated")

            if restored:
                self._log(f"Server restored subscriptions: {restored}")

            self._emit_event("connect")

            # Start heartbeat
            self._start_heartbeat()

            # Restore presence
            if self._presence:
                await self._send_presence(self._presence)

        except Exception as e:
            self._status = ConnectionStatus.DISCONNECTED
            self._log(f"Connection failed: {e}")
            raise

    def disconnect(self) -> None:
        """Disconnect from NoLag"""
        self._log("Disconnecting...")
        self._options.reconnect = False  # Prevent auto-reconnect

        self._stop_heartbeat()

        if self._reconnect_task:
            self._reconnect_task.cancel()
            self._reconnect_task = None

        if self._receive_task:
            self._receive_task.cancel()
            self._receive_task = None

        if self._ws:
            asyncio.create_task(self._ws.close())
            self._ws = None

        self._status = ConnectionStatus.DISCONNECTED
        self._presence_map.clear()

    # ============ Fluent API ============

    def set_app(self, app_name: str) -> App:
        """Set the app context for fluent API"""
        return App(self, app_name)

    # ============ Pub/Sub ============

    async def subscribe(
        self,
        topic: str,
        options: Optional[SubscribeOptions] = None,
        callback: Optional[AckCallback] = None,
    ) -> None:
        """Subscribe to a topic"""
        if not self._ws or self._status != ConnectionStatus.CONNECTED:
            if callback:
                callback(Exception("Not connected"))
            return

        opts = options or SubscribeOptions()

        message: dict[str, Any] = {
            "type": "subscribe",
            "topic": topic,
        }

        # Add QoS
        qos = opts.qos if opts.qos is not None else self._options.qos
        message["qos"] = qos.value

        # Add load balance settings
        lb = opts.load_balance if opts.load_balance is not None else self._options.load_balance
        if lb:
            message["loadBalance"] = True
            group = opts.load_balance_group or self._options.load_balance_group or self._actor_token_id
            message["loadBalanceGroup"] = group

        await self._send(message)

        if callback:
            callback(None)

    async def unsubscribe(
        self,
        topic: str,
        callback: Optional[AckCallback] = None,
    ) -> None:
        """Unsubscribe from a topic"""
        if not self._ws or self._status != ConnectionStatus.CONNECTED:
            if callback:
                callback(Exception("Not connected"))
            return

        message = {
            "type": "unsubscribe",
            "topic": topic,
        }

        await self._send(message)

        if callback:
            callback(None)

    async def emit(
        self,
        topic: str,
        data: Any,
        options: Optional[EmitOptions] = None,
        callback: Optional[AckCallback] = None,
    ) -> None:
        """Emit/publish a message to a topic"""
        if not self._ws or self._status != ConnectionStatus.CONNECTED:
            if callback:
                callback(Exception("Not connected"))
            return

        opts = options or EmitOptions()

        message: dict[str, Any] = {
            "type": "publish",
            "topic": topic,
            "data": data,
        }

        # Add QoS
        qos = opts.qos if opts.qos is not None else self._options.qos
        message["qos"] = qos.value

        if opts.retain:
            message["retain"] = True

        await self._send(message)

        if callback:
            callback(None)

    # ============ Event Handlers ============

    def on(self, event: str, handler: Callable) -> "NoLag":
        """
        Register an event handler

        For system events: 'connect', 'disconnect', 'reconnect', 'error'
        For message topics: the full topic path
        """
        if event in ("connect", "disconnect", "reconnect", "error",
                     "presence:join", "presence:leave", "presence:update"):
            if event not in self._event_handlers:
                self._event_handlers[event] = set()
            self._event_handlers[event].add(handler)
        else:
            # Topic handler
            if event not in self._message_handlers:
                self._message_handlers[event] = set()
            self._message_handlers[event].add(handler)

        return self

    def off(self, event: str, handler: Optional[Callable] = None) -> "NoLag":
        """Remove an event handler"""
        handlers = self._event_handlers if event in self._event_handlers else self._message_handlers

        if event in handlers:
            if handler:
                handlers[event].discard(handler)
            else:
                handlers[event].clear()

        return self

    def on_any(self, handler: Callable[[str, Any, MessageMeta], None]) -> "NoLag":
        """Register a handler for all messages"""
        self._any_handlers.add(handler)
        return self

    def off_any(self, handler: Optional[Callable] = None) -> "NoLag":
        """Remove an on_any handler"""
        if handler:
            self._any_handlers.discard(handler)
        else:
            self._any_handlers.clear()
        return self

    # ============ Presence ============

    async def set_presence(self, data: dict[str, Any]) -> None:
        """Set presence data for this actor"""
        self._presence = data
        if self._status == ConnectionStatus.CONNECTED:
            await self._send_presence(data)

    async def clear_presence(self) -> None:
        """Clear presence data"""
        self._presence = None
        if self._status == ConnectionStatus.CONNECTED:
            await self._send({"type": "presence", "data": None})

    def get_presence(self, actor_token_id: str) -> Optional[ActorPresence]:
        """Get presence data for an actor"""
        return self._presence_map.get(actor_token_id)

    def get_all_presence(self) -> list[ActorPresence]:
        """Get all presence data"""
        return list(self._presence_map.values())

    # ============ Private Methods ============

    async def _authenticate(self) -> Optional[list[dict]]:
        """Authenticate with the server"""
        self._auth_future = asyncio.get_event_loop().create_future()

        message: dict[str, Any] = {
            "type": "auth",
            "token": self._token,
        }

        if self._reconnect_attempts > 0:
            message["reconnect"] = True

        if self._options.load_balance:
            message["loadBalance"] = True
            if self._options.load_balance_group:
                message["loadBalanceGroup"] = self._options.load_balance_group

        await self._send(message)

        # Wait for auth response
        result = await asyncio.wait_for(self._auth_future, timeout=10.0)
        return result

    async def _send(self, message: dict) -> None:
        """Send a message to the server"""
        if not self._ws:
            return

        self._log(f"Sending: {message}")
        payload = msgpack.packb(message)
        await self._ws.send(payload)

    async def _send_presence(self, data: dict[str, Any]) -> None:
        """Send presence update"""
        await self._send({"type": "presence", "data": data})

    async def _receive_loop(self) -> None:
        """Receive messages from the server"""
        try:
            async for message in self._ws:
                await self._handle_message(message)
        except websockets.ConnectionClosed as e:
            self._log(f"Connection closed: {e}")
            await self._handle_disconnect(str(e))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._log(f"Receive error: {e}")
            await self._handle_disconnect(str(e))

    async def _handle_message(self, data: bytes) -> None:
        """Handle incoming message"""
        # Handle empty binary (heartbeat response)
        if len(data) == 0:
            self._log("Heartbeat pong received")
            return

        try:
            message = msgpack.unpackb(data)
        except Exception as e:
            self._log(f"Failed to decode message: {e}")
            return

        self._log(f"Received: {message}")

        msg_type = message.get("type")

        # Handle auth response
        if msg_type == "auth" and self._auth_future:
            if message.get("success"):
                self._actor_token_id = message.get("actorTokenId")
                self._project_id = message.get("projectId")
                actor_type = message.get("actorType")
                if actor_type:
                    self._actor_type = ActorType(actor_type)

                restored = message.get("restoredSubscriptions", [])
                self._auth_future.set_result(restored)
            else:
                error = message.get("error", "Authentication failed")
                self._auth_future.set_exception(Exception(error))
            return

        # Handle message
        if msg_type == "message":
            topic = message.get("topic", "")
            msg_data = message.get("data")
            meta = MessageMeta(
                sender=message.get("from"),
                timestamp=message.get("timestamp"),
            )

            # Call topic handlers
            if topic in self._message_handlers:
                for handler in self._message_handlers[topic]:
                    try:
                        handler(msg_data, meta)
                    except Exception as e:
                        self._log(f"Handler error: {e}")

            # Call any handlers
            for handler in self._any_handlers:
                try:
                    handler(topic, msg_data, meta)
                except Exception as e:
                    self._log(f"Any handler error: {e}")

            return

        # Handle presence
        if msg_type == "presence:join":
            actor = ActorPresence(
                actor_token_id=message.get("actorTokenId", ""),
                actor_type=ActorType(message.get("actorType", "device")),
                presence=message.get("presence", {}),
                joined_at=message.get("joinedAt"),
            )
            self._presence_map[actor.actor_token_id] = actor
            self._emit_event("presence:join", actor)
            return

        if msg_type == "presence:leave":
            actor_id = message.get("actorTokenId")
            actor = self._presence_map.pop(actor_id, None)
            if actor:
                self._emit_event("presence:leave", actor)
            return

        if msg_type == "presence:update":
            actor_id = message.get("actorTokenId")
            if actor_id in self._presence_map:
                self._presence_map[actor_id].presence = message.get("presence", {})
                self._emit_event("presence:update", self._presence_map[actor_id])
            return

        # Handle errors
        if msg_type == "error":
            error = Exception(message.get("message", "Unknown error"))
            self._emit_event("error", error)
            return

    async def _handle_disconnect(self, reason: str) -> None:
        """Handle disconnection"""
        was_connected = self._status == ConnectionStatus.CONNECTED
        self._status = ConnectionStatus.DISCONNECTED
        self._ws = None

        self._stop_heartbeat()

        if was_connected:
            self._emit_event("disconnect", reason)

        # Attempt reconnection
        if self._options.reconnect and self._reconnect_attempts < self._options.max_reconnect_attempts:
            await self._schedule_reconnect()

    async def _schedule_reconnect(self) -> None:
        """Schedule a reconnection attempt"""
        self._reconnect_attempts += 1
        self._status = ConnectionStatus.RECONNECTING

        self._log(f"Reconnecting in {self._options.reconnect_interval}s (attempt {self._reconnect_attempts})")

        await asyncio.sleep(self._options.reconnect_interval)

        try:
            await self.connect()
            self._emit_event("reconnect")
        except Exception as e:
            self._log(f"Reconnection failed: {e}")
            if self._reconnect_attempts < self._options.max_reconnect_attempts:
                await self._schedule_reconnect()

    def _emit_event(self, event: str, *args) -> None:
        """Emit an event to handlers"""
        if event in self._event_handlers:
            for handler in self._event_handlers[event]:
                try:
                    handler(*args)
                except Exception as e:
                    self._log(f"Event handler error: {e}")

    def _start_heartbeat(self) -> None:
        """Start heartbeat timer"""
        if self._options.heartbeat_interval <= 0:
            return

        self._stop_heartbeat()

        async def heartbeat_loop():
            while self._status == ConnectionStatus.CONNECTED:
                await asyncio.sleep(self._options.heartbeat_interval)
                if self._ws and self._status == ConnectionStatus.CONNECTED:
                    try:
                        await self._ws.send(b"")
                        self._log("Heartbeat ping sent")
                    except Exception as e:
                        self._log(f"Heartbeat error: {e}")
                        break

        self._heartbeat_task = asyncio.create_task(heartbeat_loop())

    def _stop_heartbeat(self) -> None:
        """Stop heartbeat timer"""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None

    def _log(self, *args) -> None:
        """Log a debug message"""
        if self._options.debug:
            logger.debug(" ".join(str(a) for a in args))
