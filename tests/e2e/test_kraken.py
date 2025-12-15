"""
E2E Tests for NoLag Python SDK with Kraken-v2

Prerequisites:
1. Start kraken-v2: cd kraken-v2 && docker-compose up -d
2. Start Titus: cd titus && npm run dev
3. Set environment variables:
   - NOLAG_TEST_TOKEN: Valid access token from Titus
   - NOLAG_TEST_TOKEN_2: Second valid access token (for multi-client tests)
   - NOLAG_TEST_URL: WebSocket URL (default: ws://localhost:8080/ws)

Run tests:
  NOLAG_TEST_TOKEN=at_xxx pytest tests/e2e/ -v
"""

import asyncio
import os
import time
from typing import Any

import pytest

from nolag import NoLag, NoLagOptions, SubscribeOptions, EmitOptions

TEST_URL = os.environ.get("NOLAG_TEST_URL", "ws://localhost:8080/ws")
TEST_TOKEN = os.environ.get("NOLAG_TEST_TOKEN")
TEST_TOKEN_2 = os.environ.get("NOLAG_TEST_TOKEN_2")


def skip_if_no_token(func):
    """Skip test if no token is provided"""
    return pytest.mark.skipif(
        not TEST_TOKEN,
        reason="NOLAG_TEST_TOKEN not set"
    )(func)


def skip_if_no_second_token(func):
    """Skip test if second token is not provided"""
    return pytest.mark.skipif(
        not TEST_TOKEN or not TEST_TOKEN_2,
        reason="NOLAG_TEST_TOKEN or NOLAG_TEST_TOKEN_2 not set"
    )(func)


async def wait_for(condition, timeout: float = 5.0, interval: float = 0.1) -> bool:
    """Wait for a condition to become true"""
    start = time.time()
    while time.time() - start < timeout:
        if condition():
            return True
        await asyncio.sleep(interval)
    return False


class TestConnection:
    """Connection-related tests"""

    @skip_if_no_token
    @pytest.mark.asyncio
    async def test_connect_and_authenticate(self):
        """Should connect and authenticate successfully"""
        client = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
        ))

        try:
            await client.connect()
            assert client.connected is True
            assert client.status.value == "connected"
            assert client.actor_id is not None
        finally:
            client.disconnect()

    @skip_if_no_token
    @pytest.mark.asyncio
    async def test_fail_with_invalid_token(self):
        """Should fail with invalid token"""
        client = NoLag("invalid_token", NoLagOptions(
            url=TEST_URL,
            reconnect=False,
        ))

        with pytest.raises(Exception):
            await client.connect()

        assert client.connected is False

    @skip_if_no_token
    @pytest.mark.asyncio
    async def test_disconnect_cleanly(self):
        """Should disconnect cleanly"""
        client = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
        ))

        await client.connect()
        assert client.connected is True

        client.disconnect()
        assert client.connected is False
        assert client.status.value == "disconnected"

    @skip_if_no_token
    @pytest.mark.asyncio
    async def test_emit_connect_event(self):
        """Should emit connect event"""
        client = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
        ))

        connect_called = False

        def on_connect():
            nonlocal connect_called
            connect_called = True

        client.on("connect", on_connect)

        try:
            await client.connect()
            assert connect_called is True
        finally:
            client.disconnect()

    @skip_if_no_token
    @pytest.mark.asyncio
    async def test_emit_disconnect_event(self):
        """Should emit disconnect event"""
        client = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
        ))

        disconnect_called = False

        def on_disconnect(reason=None):
            nonlocal disconnect_called
            disconnect_called = True

        client.on("disconnect", on_disconnect)

        await client.connect()
        client.disconnect()

        await asyncio.sleep(0.1)
        assert disconnect_called is True


class TestSubscribeUnsubscribe:
    """Subscribe/Unsubscribe tests"""

    @skip_if_no_token
    @pytest.mark.asyncio
    async def test_subscribe_to_topic(self):
        """Should subscribe to a topic"""
        client = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
        ))

        try:
            await client.connect()

            subscribe_called = False
            subscribe_error = None

            def callback(err):
                nonlocal subscribe_called, subscribe_error
                subscribe_called = True
                subscribe_error = err

            await client.subscribe("test/room/messages", callback=callback)

            assert subscribe_called is True
            assert subscribe_error is None
        finally:
            client.disconnect()

    @skip_if_no_token
    @pytest.mark.asyncio
    async def test_unsubscribe_from_topic(self):
        """Should unsubscribe from a topic"""
        client = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
        ))

        try:
            await client.connect()
            await client.subscribe("test/room/messages")

            unsubscribe_called = False
            unsubscribe_error = None

            def callback(err):
                nonlocal unsubscribe_called, unsubscribe_error
                unsubscribe_called = True
                unsubscribe_error = err

            await client.unsubscribe("test/room/messages", callback=callback)

            assert unsubscribe_called is True
            assert unsubscribe_error is None
        finally:
            client.disconnect()


class TestPubSubMessaging:
    """Publish/Subscribe messaging tests"""

    @skip_if_no_token
    @pytest.mark.asyncio
    async def test_receive_own_published_message(self):
        """Should receive own published message"""
        client = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
        ))

        try:
            await client.connect()

            topic = f"test/e2e/{int(time.time() * 1000)}"
            test_data = {"message": "hello", "timestamp": int(time.time())}
            received_data: Any = None

            def handler(data, meta):
                nonlocal received_data
                received_data = data

            await client.subscribe(topic)
            client.on(topic, handler)

            await asyncio.sleep(0.2)

            await client.emit(topic, test_data)

            success = await wait_for(lambda: received_data is not None, timeout=5.0)
            assert success, "Timeout waiting for message"
            assert received_data == test_data
        finally:
            client.disconnect()

    @skip_if_no_token
    @pytest.mark.asyncio
    async def test_multiple_handlers_same_topic(self):
        """Should handle multiple subscribers to same topic"""
        client = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
        ))

        try:
            await client.connect()

            topic = f"test/multi/{int(time.time() * 1000)}"
            handler1_called = False
            handler2_called = False

            def handler1(data, meta):
                nonlocal handler1_called
                handler1_called = True

            def handler2(data, meta):
                nonlocal handler2_called
                handler2_called = True

            await client.subscribe(topic)
            client.on(topic, handler1)
            client.on(topic, handler2)

            await asyncio.sleep(0.2)

            await client.emit(topic, {"value": 42})

            success = await wait_for(
                lambda: handler1_called and handler2_called,
                timeout=5.0
            )
            assert success, "Timeout waiting for handlers"
            assert handler1_called is True
            assert handler2_called is True
        finally:
            client.disconnect()

    @skip_if_no_token
    @pytest.mark.asyncio
    async def test_on_any_wildcard_handler(self):
        """Should work with on_any wildcard handler"""
        client = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
        ))

        try:
            await client.connect()

            topic = f"test/wildcard/{int(time.time() * 1000)}"
            test_data = {"test": True}
            received_topic = None
            received_data = None

            def any_handler(t, d, meta):
                nonlocal received_topic, received_data
                received_topic = t
                received_data = d

            await client.subscribe(topic)
            client.on_any(any_handler)

            await asyncio.sleep(0.2)

            await client.emit(topic, test_data)

            success = await wait_for(lambda: received_data is not None, timeout=5.0)
            assert success, "Timeout waiting for message"
            assert received_topic == topic
            assert received_data == test_data
        finally:
            client.disconnect()


class TestFluentAPI:
    """Fluent API (App/Room) tests"""

    @skip_if_no_token
    @pytest.mark.asyncio
    async def test_set_app_set_room_pattern(self):
        """Should work with set_app().set_room() pattern"""
        client = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
        ))

        try:
            await client.connect()

            room = client.set_app("myapp").set_room("lobby")
            test_data = {"user": "alice", "message": "hi"}
            received_data = None

            def handler(data, meta):
                nonlocal received_data
                received_data = data

            await room.subscribe("chat")
            room.on("chat", handler)

            await asyncio.sleep(0.2)

            await room.emit("chat", test_data)

            success = await wait_for(lambda: received_data is not None, timeout=5.0)
            assert success, "Timeout waiting for message"
            assert received_data == test_data
        finally:
            client.disconnect()

    @skip_if_no_token
    @pytest.mark.asyncio
    async def test_correct_prefix(self):
        """Should have correct prefix"""
        client = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            reconnect=False,
        ))

        room = client.set_app("testapp").set_room("testroom")
        assert room.prefix == "testapp/testroom"


class TestLoadBalancing:
    """Load balancing tests"""

    @skip_if_no_token
    @pytest.mark.asyncio
    async def test_subscribe_with_load_balancing(self):
        """Should subscribe with load balancing enabled"""
        client = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
            load_balance=True,
            load_balance_group="test-workers",
        ))

        try:
            await client.connect()

            assert client.load_balanced is True
            assert client.load_balance_group == "test-workers"

            await client.subscribe("jobs/process")
            await asyncio.sleep(0.1)
        finally:
            client.disconnect()

    @skip_if_no_token
    @pytest.mark.asyncio
    async def test_override_load_balance_per_subscription(self):
        """Should override load balance per subscription"""
        client = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
            load_balance=False,
        ))

        try:
            await client.connect()

            # Override for specific topic
            await client.subscribe(
                "jobs/process",
                SubscribeOptions(load_balance=True, load_balance_group="workers")
            )
            await asyncio.sleep(0.1)
        finally:
            client.disconnect()

    @skip_if_no_second_token
    @pytest.mark.asyncio
    async def test_load_balance_distribution(self):
        """Should distribute messages across load balanced subscribers"""
        # Create 2 clients in the same load balance group
        client1 = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
        ))

        client2 = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
        ))

        # Publisher client (not in load balance group)
        publisher = NoLag(TEST_TOKEN_2, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
        ))

        try:
            await asyncio.gather(
                client1.connect(),
                client2.connect(),
                publisher.connect(),
            )

            topic = f"test/loadbalance/{int(time.time() * 1000)}"
            client1_messages = []
            client2_messages = []

            def handler1(data, meta):
                client1_messages.append(data)

            def handler2(data, meta):
                client2_messages.append(data)

            # Subscribe both clients with load balancing
            await client1.subscribe(
                topic,
                SubscribeOptions(load_balance=True, load_balance_group="lb-test")
            )
            await client2.subscribe(
                topic,
                SubscribeOptions(load_balance=True, load_balance_group="lb-test")
            )

            client1.on(topic, handler1)
            client2.on(topic, handler2)

            await asyncio.sleep(0.3)

            # Publish multiple messages
            for i in range(6):
                await publisher.emit(topic, {"id": i})
                await asyncio.sleep(0.05)

            # Wait for messages to be distributed
            success = await wait_for(
                lambda: len(client1_messages) + len(client2_messages) >= 6,
                timeout=5.0
            )
            assert success, "Timeout waiting for messages"

            # Both clients should have received some messages (round-robin)
            total = len(client1_messages) + len(client2_messages)
            assert total == 6, f"Expected 6 total messages, got {total}"

            # Each client should have received approximately half
            # (might not be exactly 3 each due to timing)
            assert len(client1_messages) >= 2, "Client 1 should receive at least 2 messages"
            assert len(client2_messages) >= 2, "Client 2 should receive at least 2 messages"

        finally:
            client1.disconnect()
            client2.disconnect()
            publisher.disconnect()


class TestEcho:
    """Echo tests"""

    @skip_if_no_token
    @pytest.mark.asyncio
    async def test_receive_own_message_echo_true(self):
        """Should receive own message when echo is true (default)"""
        client = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
        ))

        try:
            await client.connect()

            topic = f"test/echo/enabled/{int(time.time() * 1000)}"
            test_data = {"message": "echo me"}
            received_data = None

            def handler(data, meta):
                nonlocal received_data
                received_data = data

            await client.subscribe(topic)
            client.on(topic, handler)

            await asyncio.sleep(0.2)

            # Emit with echo=True (default)
            await client.emit(topic, test_data)

            success = await wait_for(lambda: received_data is not None, timeout=5.0)
            assert success, "Timeout waiting for message"
            assert received_data == test_data
        finally:
            client.disconnect()

    @skip_if_no_token
    @pytest.mark.asyncio
    async def test_not_receive_own_message_echo_false(self):
        """Should NOT receive own message when echo is false"""
        client = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
        ))

        try:
            await client.connect()

            topic = f"test/echo/disabled/{int(time.time() * 1000)}"
            test_data = {"message": "no echo"}
            received_data = None

            def handler(data, meta):
                nonlocal received_data
                received_data = data

            await client.subscribe(topic)
            client.on(topic, handler)

            await asyncio.sleep(0.2)

            # Emit with echo=False
            await client.emit(topic, test_data, EmitOptions(echo=False))

            # Wait a bit to ensure message had time to arrive if it was going to
            await asyncio.sleep(0.5)

            # Should NOT have received the message
            assert received_data is None
        finally:
            client.disconnect()


class TestMultiClientMessaging:
    """Multi-client messaging tests"""

    @skip_if_no_second_token
    @pytest.mark.asyncio
    async def test_deliver_messages_between_clients(self):
        """Should deliver messages between two clients"""
        client1 = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
        ))

        client2 = NoLag(TEST_TOKEN_2, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
        ))

        try:
            await asyncio.gather(client1.connect(), client2.connect())

            topic = f"test/multiclient/{int(time.time() * 1000)}"
            test_data = {"from": "client1", "value": 123}
            client2_received = None

            def handler(data, meta):
                nonlocal client2_received
                client2_received = data

            # Client 2 subscribes
            await client2.subscribe(topic)
            client2.on(topic, handler)

            await asyncio.sleep(0.2)

            # Client 1 publishes
            await client1.emit(topic, test_data)

            success = await wait_for(lambda: client2_received is not None, timeout=5.0)
            assert success, "Timeout waiting for message"
            assert client2_received == test_data
        finally:
            client1.disconnect()
            client2.disconnect()

    @skip_if_no_second_token
    @pytest.mark.asyncio
    async def test_bidirectional_messaging(self):
        """Should support bidirectional messaging"""
        client1 = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
        ))

        client2 = NoLag(TEST_TOKEN_2, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=0,
        ))

        try:
            await asyncio.gather(client1.connect(), client2.connect())

            topic = f"test/bidirectional/{int(time.time() * 1000)}"
            client1_received = []
            client2_received = []

            def handler1(data, meta):
                client1_received.append(data)

            def handler2(data, meta):
                client2_received.append(data)

            # Both subscribe
            await client1.subscribe(topic)
            await client2.subscribe(topic)

            client1.on(topic, handler1)
            client2.on(topic, handler2)

            await asyncio.sleep(0.2)

            # Both publish
            await client1.emit(topic, {"from": "client1"})
            await client2.emit(topic, {"from": "client2"})

            success = await wait_for(
                lambda: len(client1_received) >= 2 and len(client2_received) >= 2,
                timeout=5.0
            )
            assert success, "Timeout waiting for messages"

            # Both clients should receive both messages
            assert len(client1_received) >= 2
            assert len(client2_received) >= 2
        finally:
            client1.disconnect()
            client2.disconnect()


class TestHeartbeat:
    """Heartbeat tests"""

    @skip_if_no_token
    @pytest.mark.asyncio
    async def test_heartbeat_keeps_connection(self):
        """Should send and receive heartbeat"""
        client = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            debug=True,
            reconnect=False,
            heartbeat_interval=1.0,  # 1 second for testing
        ))

        try:
            await client.connect()
            assert client.connected is True

            # Wait for at least 2 heartbeats
            await asyncio.sleep(2.5)

            # If we're still connected, heartbeats are working
            assert client.connected is True
        finally:
            client.disconnect()


class TestErrorHandling:
    """Error handling tests"""

    @skip_if_no_token
    @pytest.mark.asyncio
    async def test_handle_subscribe_before_connect(self):
        """Should handle subscribe before connect"""
        client = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            reconnect=False,
        ))

        error_received = False

        def callback(err):
            nonlocal error_received
            if err:
                error_received = True

        await client.subscribe("test/topic", callback=callback)

        assert error_received is True

    @skip_if_no_token
    @pytest.mark.asyncio
    async def test_handle_emit_before_connect(self):
        """Should handle emit before connect"""
        client = NoLag(TEST_TOKEN, NoLagOptions(
            url=TEST_URL,
            reconnect=False,
        ))

        error_received = False

        def callback(err):
            nonlocal error_received
            if err:
                error_received = True

        await client.emit("test/topic", {"data": "test"}, callback=callback)

        assert error_received is True
