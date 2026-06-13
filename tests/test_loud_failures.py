"""Unit tests for the loud-failure behaviors (no broker required).

Mirrors js-sdk tests/unit/loud-failures.test.ts:
 - encode errors raise NoLagEncodeError with op/topic context
 - not-open sends raise instead of silently dropping
 - subscribe callbacks resolve on real `subscribed` frames and reject on
   topic-matched error frames
 - publish callbacks correlate msgRef `published` acks on v2
 - server error frames surface as structured NoLagServerError
 - protocolVersion negotiation falls back to 1 for pre-v2 brokers
"""
from __future__ import annotations

import asyncio

import msgpack
import pytest

from nolag import NoLag, NoLagEncodeError, NoLagServerError
from nolag.types import ConnectionStatus


class FakeWs:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send(self, payload: bytes) -> None:
        self.sent.append(msgpack.unpackb(payload))


def make_connected_client(protocol_version: int = 2) -> tuple[NoLag, FakeWs]:
    client = NoLag("test-token")
    ws = FakeWs()
    client._ws = ws  # type: ignore[assignment]
    client._status = ConnectionStatus.CONNECTED
    client._protocol_version = protocol_version
    return client, ws


async def receive(client: NoLag, frame: dict) -> None:
    await client._handle_message(msgpack.packb(frame))


class TestLoudSends:
    async def test_encode_error_raises_with_topic_context(self):
        client, _ws = make_connected_client()
        errors: list[Exception] = []
        client.on("error", lambda e: errors.append(e))

        class NotSerializable:
            pass

        with pytest.raises(NoLagEncodeError) as exc:
            await client.emit("app/room/topic", NotSerializable())
        assert "app/room/topic" in str(exc.value)
        assert errors and isinstance(errors[0], NoLagEncodeError)

    async def test_encode_error_routed_to_callback(self):
        client, _ws = make_connected_client()
        client.on("error", lambda e: None)
        results: list = []

        class NotSerializable:
            pass

        await client.emit("app/room/topic", NotSerializable(), callback=results.append)
        assert len(results) == 1
        assert isinstance(results[0], NoLagEncodeError)

    async def test_not_connected_emit_errors_callback(self):
        client = NoLag("test-token")
        results: list = []
        await client.emit("app/room/topic", {"a": 1}, callback=results.append)
        assert len(results) == 1
        assert isinstance(results[0], Exception)

    async def test_send_raises_when_ws_missing(self):
        client, _ws = make_connected_client()
        client._ws = None
        with pytest.raises(ConnectionError):
            await client._send({"type": "publish"}, "publish", "app/room/topic")


class TestSubscribeAcks:
    async def test_resolves_on_subscribed_frame(self):
        client, _ws = make_connected_client()
        results: list = []
        await client.subscribe("app/room/topic", callback=results.append)
        assert results == []  # no optimistic callback(None)

        await receive(client, {"type": "subscribed", "topic": "app/room/topic"})
        assert results == [None]

    async def test_rejects_on_topic_matched_error_frame(self):
        client, _ws = make_connected_client()
        client.on("error", lambda e: None)
        results: list = []
        await client.subscribe("app/forbidden/topic", callback=results.append)

        await receive(client, {"type": "error", "error": "not_authorized", "topic": "app/forbidden/topic"})
        assert len(results) == 1
        err = results[0]
        assert isinstance(err, NoLagServerError)
        assert err.error == "not_authorized"
        assert err.topic == "app/forbidden/topic"

    async def test_42940_carries_hint(self):
        client, _ws = make_connected_client()
        client.on("error", lambda e: None)
        results: list = []
        await client.subscribe("app/new-room/events", callback=results.append)

        await receive(client, {
            "type": "error",
            "code": 42940,
            "error": "unknown_topic",
            "topic": "app/new-room/events",
            "hint": "room is not configured and auto-provisioning is not available",
        })
        err = results[0]
        assert err.code == 42940
        assert "auto-provisioning" in str(err)


class TestPublishAcks:
    async def test_v2_attaches_msg_ref_and_resolves_on_published(self):
        client, ws = make_connected_client(protocol_version=2)
        results: list = []
        await client.emit("app/room/topic", {"a": 1}, callback=results.append)
        assert results == []

        publish = next(m for m in ws.sent if m["type"] == "publish")
        assert publish.get("msgRef")

        await receive(client, {"type": "published", "topic": "app/room/topic", "msgRef": publish["msgRef"]})
        assert results == [None]

    async def test_v2_rejects_on_msg_ref_matched_error(self):
        client, ws = make_connected_client(protocol_version=2)
        client.on("error", lambda e: None)
        results: list = []
        await client.emit("app/room/topic", {"a": 1}, callback=results.append)
        publish = next(m for m in ws.sent if m["type"] == "publish")

        await receive(client, {
            "type": "error", "code": 42940, "error": "unknown_topic",
            "topic": "app/room/topic", "msgRef": publish["msgRef"],
        })
        assert isinstance(results[0], NoLagServerError)
        assert results[0].code == 42940

    async def test_v1_keeps_optimistic_semantics(self):
        client, ws = make_connected_client(protocol_version=1)
        results: list = []
        await client.emit("app/room/topic", {"a": 1}, callback=results.append)
        assert results == [None]
        publish = next(m for m in ws.sent if m["type"] == "publish")
        assert "msgRef" not in publish


class TestServerErrors:
    async def test_unsolicited_error_frame_is_structured(self):
        client, _ws = make_connected_client()
        errors: list = []
        client.on("error", lambda e: errors.append(e))

        await receive(client, {"type": "error", "code": 42910, "error": "rate_limit_exceeded", "topic": "t"})
        assert isinstance(errors[0], NoLagServerError)
        assert errors[0].code == 42910
        assert "rate_limit_exceeded" in str(errors[0])


class TestVersionNegotiation:
    async def test_auth_sends_protocol_version(self):
        client, ws = make_connected_client()
        client._status = ConnectionStatus.CONNECTING

        auth_task = asyncio.ensure_future(client._authenticate())
        await asyncio.sleep(0.01)
        auth = next(m for m in ws.sent if m["type"] == "auth")
        assert auth["protocolVersion"] == 2

        await receive(client, {
            "type": "auth", "success": True, "actorTokenId": "a1",
            "projectId": "p1", "restoredSubscriptions": [], "protocolVersion": 2,
        })
        await auth_task
        assert client.protocol_version == 2

    async def test_falls_back_to_v1_when_broker_omits_version(self):
        client, ws = make_connected_client()
        client._status = ConnectionStatus.CONNECTING

        auth_task = asyncio.ensure_future(client._authenticate())
        await asyncio.sleep(0.01)
        await receive(client, {
            "type": "auth", "success": True, "actorTokenId": "a1",
            "projectId": "p1", "restoredSubscriptions": [],
        })
        await auth_task
        assert client.protocol_version == 1
