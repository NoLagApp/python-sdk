"""
WebRTC Manager for NoLag Python SDK

Provides peer-to-peer video/audio connections using NoLag as the signaling server.
Uses the "Perfect Negotiation" pattern to handle offer collisions gracefully.

Requires aiortc: pip install aiortc

Example:
    from nolag import NoLag
    from nolag.webrtc import WebRTCManager

    client = NoLag('your_access_token')
    await client.connect()

    webrtc = WebRTCManager(client, app='video-chat', room='meeting-123')

    @webrtc.on('peer_connected')
    async def on_peer(actor_id, stream):
        # Process incoming audio with speech-to-text
        pass

    await webrtc.start()
"""

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum

logger = logging.getLogger("nolag.webrtc")

# Try to import aiortc
try:
    from aiortc import RTCPeerConnection, RTCSessionDescription, RTCIceCandidate
    from aiortc.contrib.media import MediaStreamTrack
    AIORTC_AVAILABLE = True
except ImportError:
    AIORTC_AVAILABLE = False
    RTCPeerConnection = None
    RTCSessionDescription = None
    RTCIceCandidate = None


# Default STUN servers
DEFAULT_ICE_SERVERS = [
    {"urls": "stun:stun.l.google.com:19302"},
    {"urls": "stun:stun1.l.google.com:19302"},
]


@dataclass
class WebRTCOptions:
    """Options for WebRTCManager"""
    app: str
    room: str
    ice_servers: list[dict] = field(default_factory=lambda: DEFAULT_ICE_SERVERS.copy())


@dataclass
class PeerState:
    """State for a peer connection"""
    actor_id: str
    pc: Any  # RTCPeerConnection
    polite: bool
    making_offer: bool = False
    ignore_offer: bool = False
    remote_stream: Optional[Any] = None


class WebRTCEvent(str, Enum):
    """WebRTC events"""
    PEER_CONNECTED = "peer_connected"
    PEER_DISCONNECTED = "peer_disconnected"
    PEER_TRACK = "peer_track"
    LOCAL_STREAM = "local_stream"
    ERROR = "error"


class WebRTCManager:
    """
    WebRTC Manager

    Manages peer-to-peer WebRTC connections using NoLag for signaling.
    Uses aiortc for WebRTC implementation in Python.

    Example:
        webrtc = WebRTCManager(client, app='video-chat', room='meeting-123')

        @webrtc.on('peer_connected')
        async def on_peer(actor_id, tracks):
            for track in tracks:
                if track.kind == 'audio':
                    # Process audio
                    pass

        await webrtc.start()
    """

    def __init__(
        self,
        client: "NoLag",
        app: str,
        room: str,
        ice_servers: Optional[list[dict]] = None,
    ):
        if not AIORTC_AVAILABLE:
            raise ImportError(
                "aiortc is required for WebRTC support. Install it with: pip install aiortc"
            )

        self._client = client
        self._options = WebRTCOptions(
            app=app,
            room=room,
            ice_servers=ice_servers or DEFAULT_ICE_SERVERS.copy(),
        )

        # Create room context
        self._room = client.set_app(app).set_room(room)

        self._local_tracks: list[Any] = []
        self._peers: dict[str, PeerState] = {}
        self._started = False
        self._event_handlers: dict[str, set[Callable]] = {}

    # ============ Properties ============

    @property
    def topic_prefix(self) -> str:
        """The full topic prefix (app/room)"""
        return f"{self._options.app}/{self._options.room}"

    @property
    def my_actor_id(self) -> Optional[str]:
        """Local actor ID"""
        return self._client.actor_id

    @property
    def is_started(self) -> bool:
        """Whether the manager is started"""
        return self._started

    # ============ Public API ============

    def add_track(self, track: Any) -> None:
        """Add a local track to share with peers"""
        self._local_tracks.append(track)
        self._emit("local_stream", track)

        # Add to existing peer connections
        for peer in self._peers.values():
            peer.pc.addTrack(track)

    def get_peers(self) -> list[str]:
        """Get list of connected peer IDs"""
        return list(self._peers.keys())

    def is_connected(self, actor_id: str) -> bool:
        """Check if connected to a specific peer"""
        peer = self._peers.get(actor_id)
        if not peer:
            return False
        return peer.pc.connectionState == "connected"

    async def start(self) -> None:
        """
        Start the WebRTC manager

        - Subscribes to signaling topics
        - Listens for presence events
        - Initiates connections to existing peers
        """
        if self._started:
            raise RuntimeError("WebRTCManager already started")

        if not self._client.connected:
            raise RuntimeError("NoLag client not connected")

        if not self.my_actor_id:
            raise RuntimeError("Actor ID not available")

        self._started = True

        # Subscribe to signaling topics
        topics = ["webrtc:offer", "webrtc:answer", "webrtc:candidate"]
        for topic in topics:
            await self._room.subscribe(topic)

        # Listen to signaling messages
        self._room.on("webrtc:offer", self._handle_offer)
        self._room.on("webrtc:answer", self._handle_answer)
        self._room.on("webrtc:candidate", self._handle_candidate)

        # Listen to presence events
        self._client.on("presence:join", self._on_presence_join)
        self._client.on("presence:leave", self._on_presence_leave)

        # Set presence with webrtcReady flag
        await self._client.set_presence({"webrtcReady": True})

        # Note: In Python SDK we don't have fetch_presence yet
        # Peers will be discovered via presence:join events

    async def stop(self) -> None:
        """
        Stop the WebRTC manager

        - Closes all peer connections
        - Unsubscribes from signaling topics
        - Removes event listeners
        """
        if not self._started:
            return

        self._started = False

        # Close all peer connections
        for actor_id, peer in list(self._peers.items()):
            await peer.pc.close()
            self._emit("peer_disconnected", actor_id)
        self._peers.clear()

        # Unsubscribe from signaling topics
        topics = ["webrtc:offer", "webrtc:answer", "webrtc:candidate"]
        for topic in topics:
            await self._room.unsubscribe(topic)

        # Remove handlers
        self._room.off("webrtc:offer")
        self._room.off("webrtc:answer")
        self._room.off("webrtc:candidate")
        self._client.off("presence:join")
        self._client.off("presence:leave")

        # Clear presence
        await self._client.set_presence({"webrtcReady": False})

    # ============ Event Handlers ============

    def on(self, event: str, handler: Callable) -> "WebRTCManager":
        """Register an event handler"""
        if event not in self._event_handlers:
            self._event_handlers[event] = set()
        self._event_handlers[event].add(handler)
        return self

    def off(self, event: str, handler: Optional[Callable] = None) -> "WebRTCManager":
        """Remove an event handler"""
        if event in self._event_handlers:
            if handler:
                self._event_handlers[event].discard(handler)
            else:
                self._event_handlers[event].clear()
        return self

    def _emit(self, event: str, *args) -> None:
        """Emit an event"""
        if event in self._event_handlers:
            for handler in self._event_handlers[event]:
                try:
                    result = handler(*args)
                    if asyncio.iscoroutine(result):
                        asyncio.create_task(result)
                except Exception as e:
                    logger.error(f"Error in WebRTC event handler for {event}: {e}")

    # ============ Private Methods ============

    def _on_presence_join(self, actor: Any) -> None:
        """Handle presence join event"""
        actor_id = actor.actor_token_id
        if (
            actor_id != self.my_actor_id
            and actor.presence.get("webrtcReady")
            and actor_id not in self._peers
        ):
            asyncio.create_task(self._create_peer_connection(actor_id))

    def _on_presence_leave(self, actor: Any) -> None:
        """Handle presence leave event"""
        actor_id = actor.actor_token_id
        peer = self._peers.pop(actor_id, None)
        if peer:
            asyncio.create_task(peer.pc.close())
            self._emit("peer_disconnected", actor_id)

    async def _create_peer_connection(self, remote_actor_id: str) -> PeerState:
        """Create a new peer connection"""
        config = {
            "iceServers": self._options.ice_servers,
        }

        pc = RTCPeerConnection(configuration=config)

        # Determine politeness for perfect negotiation
        polite = self.my_actor_id < remote_actor_id

        peer_state = PeerState(
            actor_id=remote_actor_id,
            pc=pc,
            polite=polite,
        )

        self._peers[remote_actor_id] = peer_state

        # Add local tracks
        for track in self._local_tracks:
            pc.addTrack(track)

        # Handle ICE candidates
        @pc.on("icecandidate")
        async def on_ice_candidate(candidate):
            if candidate:
                await self._send_candidate(remote_actor_id, {
                    "candidate": candidate.candidate,
                    "sdpMid": candidate.sdpMid,
                    "sdpMLineIndex": candidate.sdpMLineIndex,
                })

        # Handle remote tracks
        @pc.on("track")
        def on_track(track):
            peer_state.remote_stream = track
            self._emit("peer_track", remote_actor_id, track)
            self._emit("peer_connected", remote_actor_id, track)

        # Handle connection state changes
        @pc.on("connectionstatechange")
        async def on_connection_state_change():
            if pc.connectionState in ("disconnected", "failed", "closed"):
                self._peers.pop(remote_actor_id, None)
                self._emit("peer_disconnected", remote_actor_id)

        # Handle negotiation needed
        @pc.on("negotiationneeded")
        async def on_negotiation_needed():
            try:
                peer_state.making_offer = True
                offer = await pc.createOffer()
                await pc.setLocalDescription(offer)
                await self._send_offer(remote_actor_id, pc.localDescription)
            except Exception as e:
                self._emit("error", e)
            finally:
                peer_state.making_offer = False

        return peer_state

    def _handle_offer(self, data: dict, meta: Any) -> None:
        """Handle incoming offer"""
        asyncio.create_task(self._handle_offer_async(data, meta))

    async def _handle_offer_async(self, data: dict, meta: Any) -> None:
        """Handle incoming offer (async)"""
        if data.get("targetActorId") != self.my_actor_id:
            return

        sender_actor_id = data.get("senderActorId")
        if not sender_actor_id:
            return

        peer = self._peers.get(sender_actor_id)
        if not peer:
            peer = await self._create_peer_connection(sender_actor_id)

        pc = peer.pc

        # Perfect negotiation: handle offer collision
        offer_collision = peer.making_offer or pc.signalingState != "stable"
        peer.ignore_offer = not peer.polite and offer_collision

        if peer.ignore_offer:
            return

        try:
            offer = RTCSessionDescription(sdp=data["sdp"], type="offer")
            await pc.setRemoteDescription(offer)
            answer = await pc.createAnswer()
            await pc.setLocalDescription(answer)
            await self._send_answer(sender_actor_id, pc.localDescription, data.get("sessionId", ""))
        except Exception as e:
            self._emit("error", e)

    def _handle_answer(self, data: dict, meta: Any) -> None:
        """Handle incoming answer"""
        asyncio.create_task(self._handle_answer_async(data, meta))

    async def _handle_answer_async(self, data: dict, meta: Any) -> None:
        """Handle incoming answer (async)"""
        if data.get("targetActorId") != self.my_actor_id:
            return

        sender_actor_id = data.get("senderActorId")
        if not sender_actor_id:
            return

        peer = self._peers.get(sender_actor_id)
        if not peer:
            return

        try:
            answer = RTCSessionDescription(sdp=data["sdp"], type="answer")
            await peer.pc.setRemoteDescription(answer)
        except Exception as e:
            self._emit("error", e)

    def _handle_candidate(self, data: dict, meta: Any) -> None:
        """Handle incoming ICE candidate"""
        asyncio.create_task(self._handle_candidate_async(data, meta))

    async def _handle_candidate_async(self, data: dict, meta: Any) -> None:
        """Handle incoming ICE candidate (async)"""
        if data.get("targetActorId") != self.my_actor_id:
            return

        sender_actor_id = data.get("senderActorId")
        if not sender_actor_id:
            return

        peer = self._peers.get(sender_actor_id)
        if not peer or peer.ignore_offer:
            return

        try:
            candidate_data = data.get("candidate", {})
            candidate = RTCIceCandidate(
                candidate=candidate_data.get("candidate"),
                sdpMid=candidate_data.get("sdpMid"),
                sdpMLineIndex=candidate_data.get("sdpMLineIndex"),
            )
            await peer.pc.addIceCandidate(candidate)
        except Exception as e:
            if not peer.ignore_offer:
                self._emit("error", e)

    async def _send_offer(self, target_actor_id: str, description: Any) -> None:
        """Send an offer to a peer"""
        session_id = f"sess_{uuid.uuid4().hex[:16]}"
        message = {
            "type": "offer",
            "senderActorId": self.my_actor_id,
            "targetActorId": target_actor_id,
            "sessionId": session_id,
            "sdp": description.sdp,
        }
        await self._room.emit("webrtc:offer", message, options={"echo": False})

    async def _send_answer(self, target_actor_id: str, description: Any, session_id: str) -> None:
        """Send an answer to a peer"""
        message = {
            "type": "answer",
            "senderActorId": self.my_actor_id,
            "targetActorId": target_actor_id,
            "sessionId": session_id,
            "sdp": description.sdp,
        }
        await self._room.emit("webrtc:answer", message, options={"echo": False})

    async def _send_candidate(self, target_actor_id: str, candidate: dict) -> None:
        """Send an ICE candidate to a peer"""
        message = {
            "senderActorId": self.my_actor_id,
            "targetActorId": target_actor_id,
            "candidate": candidate,
        }
        await self._room.emit("webrtc:candidate", message, options={"echo": False})


def is_webrtc_available() -> bool:
    """Check if WebRTC (aiortc) is available"""
    return AIORTC_AVAILABLE
