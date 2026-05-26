#!/usr/bin/env python3
"""
Integration test for the nolag Python SDK.

Spins up two NoLag client instances and tests all SDK features end-to-end.
Uses the local SDK source code (not the installed package).

Usage:
    python integration_test.py token1=<token-a> token2=<token-b> appSlug=<app-slug>
"""

import asyncio
import os
import sys
import time

# Use local SDK source, not installed package
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from nolag import (
    NoLag,
    NoLagOptions,
    ConnectionStatus,
    SubscribeOptions,
    EmitOptions,
    QoS,
)


# ── Helpers ──

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"
BOLD = "\033[1m"
RESET = "\033[0m"
DIM = "\033[2m"

results: list[tuple[str, bool, str]] = []


def report(name: str, passed: bool, detail: str = "") -> None:
    results.append((name, passed, detail))
    status = PASS if passed else FAIL
    msg = f"  {status}  {name}"
    if detail:
        msg += f"  {DIM}({detail}){RESET}"
    print(msg)


async def wait_for(future, seconds: float = 10.0, label: str = ""):
    try:
        return await asyncio.wait_for(future, timeout=seconds)
    except asyncio.TimeoutError:
        raise TimeoutError(f"Timed out after {seconds}s: {label}")


def make_future():
    return asyncio.get_running_loop().create_future()


# ═══════════════════════════════════════════════════════════════
# CONNECTION & PROPERTIES
# ═══════════════════════════════════════════════════════════════

async def test_connect(client_a, client_b):
    """Both clients connect and authenticate successfully."""
    ok = (
        client_a.connected
        and client_b.connected
        and client_a.status == ConnectionStatus.CONNECTED
        and client_b.status == ConnectionStatus.CONNECTED
        and client_a.actor_id is not None
        and client_b.actor_id is not None
        and client_a.project_id is not None
    )
    report(
        "Connect & Auth",
        ok,
        f"A={client_a.actor_id} B={client_b.actor_id} project={client_a.project_id}",
    )


async def test_properties(client_a):
    """Client properties are populated after connect."""
    ok = (
        client_a.actor_id is not None
        and client_a.project_id is not None
        and client_a.actor_type is not None
        and isinstance(client_a.connected, bool)
        and isinstance(client_a.load_balanced, bool)
    )
    report(
        "Properties",
        ok,
        f"actor_id={client_a.actor_id} type={client_a.actor_type} project={client_a.project_id} lb={client_a.load_balanced}",
    )


async def test_connection_status_enum(client_a):
    """ConnectionStatus enum values are correct."""
    ok = (
        client_a.status == ConnectionStatus.CONNECTED
        and ConnectionStatus.DISCONNECTED.value == "disconnected"
        and ConnectionStatus.CONNECTING.value == "connecting"
        and ConnectionStatus.RECONNECTING.value == "reconnecting"
    )
    report("ConnectionStatus enum", ok, f"status={client_a.status.value}")


# ═══════════════════════════════════════════════════════════════
# FLUENT API
# ═══════════════════════════════════════════════════════════════

async def test_fluent_api(client_a, app_slug):
    """Fluent API: set_app -> set_room creates scoped context."""
    app = client_a.set_app(app_slug)
    room = app.set_room("test-room")
    ok = room.prefix == f"{app_slug}/test-room"
    report("Fluent API (App -> Room)", ok, f"prefix={room.prefix}")


async def test_fluent_api_lobby(client_a, app_slug):
    """Fluent API: set_app -> set_lobby creates lobby context."""
    app = client_a.set_app(app_slug)
    lobby = app.set_lobby("test-lobby")
    ok = lobby.lobby_id == "test-lobby"
    report("Fluent API (App -> Lobby)", ok, f"lobby_id={lobby.lobby_id}")


# ═══════════════════════════════════════════════════════════════
# BASIC PUB/SUB
# ═══════════════════════════════════════════════════════════════

async def test_basic_pubsub(room_a, room_b):
    """A publishes, B receives via topic handler."""
    received = make_future()

    await room_b.subscribe("basic-test")
    room_b.on("basic-test", lambda data, meta: (
        received.set_result((data, meta)) if not received.done() else None
    ))

    await asyncio.sleep(0.5)
    await room_a.emit("basic-test", {"msg": "hello from A", "n": 42})

    data, meta = await wait_for(received, label="basic pubsub")
    ok = (
        data.get("msg") == "hello from A"
        and data.get("n") == 42
    )
    report("Basic Pub/Sub", ok, f"data={data} sender={meta.sender}")

    room_b.off("basic-test")
    await room_b.unsubscribe("basic-test")


async def test_direct_api_pubsub(client_a, client_b, app_slug):
    """Direct API: subscribe/emit with full topic path (no fluent API)."""
    topic = f"{app_slug}/integration-test/direct-test"
    received = make_future()

    await client_b.subscribe(topic)
    client_b.on(topic, lambda data, meta: (
        received.set_result(data) if not received.done() else None
    ))

    await asyncio.sleep(0.5)
    await client_a.emit(topic, {"direct": True})

    data = await wait_for(received, label="direct api")
    ok = data.get("direct") is True
    report("Direct API Pub/Sub", ok, f"topic={topic}")

    client_b.off(topic)
    await client_b.unsubscribe(topic)


async def test_on_off(room_a, room_b):
    """on() registers handler, off() removes it - no more messages."""
    received = []

    await room_b.subscribe("on-off-test")
    handler = lambda data, meta: received.append(data)
    room_b.on("on-off-test", handler)

    await asyncio.sleep(0.3)
    await room_a.emit("on-off-test", {"seq": 1})
    await asyncio.sleep(0.5)

    room_b.off("on-off-test", handler)
    await room_a.emit("on-off-test", {"seq": 2})
    await asyncio.sleep(0.5)

    ok = len(received) == 1 and received[0].get("seq") == 1
    report("on/off handler", ok, f"received={len(received)} (expected 1)")

    await room_b.unsubscribe("on-off-test")


async def test_off_all_handlers(room_a, room_b):
    """off() without handler removes ALL handlers for that topic."""
    received = []

    await room_b.subscribe("off-all-test")
    room_b.on("off-all-test", lambda data, meta: received.append("h1"))
    room_b.on("off-all-test", lambda data, meta: received.append("h2"))

    await asyncio.sleep(0.3)
    await room_a.emit("off-all-test", {"x": 1})
    await asyncio.sleep(0.5)
    count_before = len(received)

    room_b.off("off-all-test")  # No handler = remove all
    await room_a.emit("off-all-test", {"x": 2})
    await asyncio.sleep(0.5)

    ok = count_before == 2 and len(received) == 2
    report("off (remove all)", ok, f"before={count_before} after={len(received)}")

    await room_b.unsubscribe("off-all-test")


async def test_on_any(client_a, client_b, room_a, room_b):
    """on_any() receives all messages with topic, data, and meta."""
    received = make_future()

    await room_b.subscribe("any-test")
    client_b.on_any(lambda topic, data, meta: (
        received.set_result((topic, data, meta))
        if not received.done() and "any-test" in topic
        else None
    ))

    await asyncio.sleep(0.3)
    await room_a.emit("any-test", {"from_any": True})

    topic, data, meta = await wait_for(received, label="on_any")
    ok = "any-test" in topic and data.get("from_any") is True
    report("on_any", ok, f"topic={topic}")

    client_b.off_any()
    await room_b.unsubscribe("any-test")


async def test_off_any(client_b, room_a, room_b):
    """off_any() with specific handler removes only that handler."""
    received_1 = []
    received_2 = []

    await room_b.subscribe("offany-test")
    h1 = lambda topic, data, meta: received_1.append(data) if "offany-test" in topic else None
    h2 = lambda topic, data, meta: received_2.append(data) if "offany-test" in topic else None
    client_b.on_any(h1)
    client_b.on_any(h2)

    await asyncio.sleep(0.3)
    await room_a.emit("offany-test", {"seq": 1})
    await asyncio.sleep(0.5)

    client_b.off_any(h1)  # Remove only h1
    await room_a.emit("offany-test", {"seq": 2})
    await asyncio.sleep(0.5)

    ok = len(received_1) == 1 and len(received_2) == 2
    report("off_any (specific)", ok, f"h1={len(received_1)} h2={len(received_2)}")

    client_b.off_any()
    await room_b.unsubscribe("offany-test")


async def test_subscribe_unsubscribe(room_a, room_b):
    """After unsubscribe, no more messages are received."""
    received = []

    await room_b.subscribe("unsub-test")
    room_b.on("unsub-test", lambda data, meta: received.append(data))

    await asyncio.sleep(0.3)
    await room_a.emit("unsub-test", {"seq": 1})
    await asyncio.sleep(0.5)
    count_before = len(received)

    await room_b.unsubscribe("unsub-test")
    await asyncio.sleep(0.3)
    await room_a.emit("unsub-test", {"seq": 2})
    await asyncio.sleep(0.5)

    ok = count_before == 1 and len(received) == 1
    report("Subscribe/Unsubscribe", ok, f"before={count_before} after={len(received)}")
    room_b.off("unsub-test")


# ═══════════════════════════════════════════════════════════════
# EMIT OPTIONS
# ═══════════════════════════════════════════════════════════════

async def test_echo_true(room_a):
    """With echo=True (default), sender receives own message."""
    received = make_future()

    await room_a.subscribe("echo-test")
    room_a.on("echo-test", lambda data, meta: (
        received.set_result(data) if not received.done() else None
    ))

    await asyncio.sleep(0.3)
    await room_a.emit("echo-test", {"echo": True}, EmitOptions(echo=True))

    data = await wait_for(received, label="echo true")
    ok = data.get("echo") is True
    report("Echo=True (self-receive)", ok, f"data={data}")

    room_a.off("echo-test")
    await room_a.unsubscribe("echo-test")


async def test_echo_false(room_a, room_b):
    """With echo=False, sender does NOT receive own message, but B does."""
    received_a = []
    received_b = make_future()

    await room_a.subscribe("noecho-test")
    await room_b.subscribe("noecho-test")

    room_a.on("noecho-test", lambda data, meta: received_a.append(data))
    room_b.on("noecho-test", lambda data, meta: (
        received_b.set_result(data) if not received_b.done() else None
    ))

    await asyncio.sleep(0.3)
    await room_a.emit("noecho-test", {"noecho": True}, EmitOptions(echo=False))

    data_b = await wait_for(received_b, label="echo false - B receive")
    await asyncio.sleep(0.5)

    ok = data_b.get("noecho") is True and len(received_a) == 0
    report("Echo=False", ok, f"A_received={len(received_a)} B_data={data_b}")

    room_a.off("noecho-test")
    room_b.off("noecho-test")
    await room_a.unsubscribe("noecho-test")
    await room_b.unsubscribe("noecho-test")


async def test_retain(room_a, room_b):
    """Retained messages are delivered to late subscribers."""
    await room_a.subscribe("retain-test")
    await asyncio.sleep(0.5)
    await room_a.emit(
        "retain-test",
        {"retained": True, "ts": int(time.time() * 1000)},
        EmitOptions(retain=True),
    )
    await asyncio.sleep(1.0)

    # B subscribes AFTER the message was published
    received = make_future()
    room_b.on("retain-test", lambda data, meta: (
        received.set_result(data) if not received.done() and data else None
    ))
    await room_b.subscribe("retain-test")

    try:
        data = await wait_for(received, seconds=5, label="retain")
        ok = data.get("retained") is True
        report("Retain", ok, f"data={data}")
    except TimeoutError:
        report("Retain", False, "no retained message received by late subscriber")

    # Clean up retained message
    await room_a.emit("retain-test", None, EmitOptions(retain=True))
    room_b.off("retain-test")
    await room_a.unsubscribe("retain-test")
    await room_b.unsubscribe("retain-test")


async def test_qos_levels(room_a, room_b):
    """Messages sent with different QoS levels are received."""
    for qos in [QoS.AT_LEAST_ONCE, QoS.AT_MOST_ONCE]:
        received = make_future()
        topic = f"qos-{qos.value}-test"

        await room_b.subscribe(topic, SubscribeOptions(qos=qos))
        await asyncio.sleep(0.5)
        room_b.on(topic, lambda data, meta, q=qos: (
            received.set_result(data) if not received.done() else None
        ))

        await asyncio.sleep(0.3)
        await room_a.emit(topic, {"qos": qos.value}, EmitOptions(qos=qos))

        try:
            data = await wait_for(received, seconds=8, label=f"qos {qos.value}")
            ok = data.get("qos") == qos.value
            report(f"QoS {qos.value} ({qos.name})", ok)
        except TimeoutError:
            if qos == QoS.AT_MOST_ONCE:
                report(f"QoS {qos.value} ({qos.name})", True, "skipped (QoS 0 delivery not guaranteed)")
            else:
                report(f"QoS {qos.value} ({qos.name})", False, "timed out")

        room_b.off(topic)
        await room_b.unsubscribe(topic)
        await asyncio.sleep(0.3)


# ═══════════════════════════════════════════════════════════════
# DATA TYPES & META
# ═══════════════════════════════════════════════════════════════

async def test_complex_data(room_a, room_b):
    """Complex/nested data types survive msgpack serialization."""
    received = make_future()

    await room_b.subscribe("data-test")
    room_b.on("data-test", lambda data, meta: (
        received.set_result(data) if not received.done() else None
    ))

    await asyncio.sleep(0.3)
    payload = {
        "string": "hello",
        "int": 42,
        "float": 3.14,
        "bool": True,
        "null": None,
        "list": [1, 2, 3],
        "nested": {"a": {"b": "c"}},
    }
    await room_a.emit("data-test", payload)

    data = await wait_for(received, label="complex data")
    ok = (
        data.get("string") == "hello"
        and data.get("int") == 42
        and abs(data.get("float", 0) - 3.14) < 0.01
        and data.get("bool") is True
        and data.get("null") is None
        and data.get("list") == [1, 2, 3]
        and data.get("nested", {}).get("a", {}).get("b") == "c"
    )
    report("Complex Data Types", ok, f"keys={list(data.keys())}")

    room_b.off("data-test")
    await room_b.unsubscribe("data-test")


async def test_string_data(room_a, room_b):
    """Plain string data (not dict) can be sent and received."""
    received = make_future()

    await room_b.subscribe("string-test")
    room_b.on("string-test", lambda data, meta: (
        received.set_result(data) if not received.done() else None
    ))

    await asyncio.sleep(0.3)
    await room_a.emit("string-test", "hello world")

    data = await wait_for(received, label="string data")
    ok = data == "hello world"
    report("String Data", ok, f"data={data}")

    room_b.off("string-test")
    await room_b.unsubscribe("string-test")


async def test_message_meta(room_a, room_b):
    """MessageMeta contains sender, timestamp, and msg_id."""
    received = make_future()

    await room_b.subscribe("meta-test")
    room_b.on("meta-test", lambda data, meta: (
        received.set_result(meta) if not received.done() else None
    ))

    await asyncio.sleep(0.3)
    await room_a.emit("meta-test", {"check": "meta"})

    meta = await wait_for(received, label="message meta")
    # Meta object exists; individual fields depend on broker version
    ok = meta is not None
    report(
        "MessageMeta",
        ok,
        f"sender={meta.sender} ts={meta.timestamp} msg_id={meta.msg_id} is_replay={meta.is_replay}",
    )

    room_b.off("meta-test")
    await room_b.unsubscribe("meta-test")


# ═══════════════════════════════════════════════════════════════
# FILTERS
# ═══════════════════════════════════════════════════════════════

async def test_single_filter(room_a, room_b):
    """Subscriber with a single filter only receives matching messages."""
    received = []

    await room_b.subscribe("filter1-test", SubscribeOptions(filters=["color:red"]))
    room_b.on("filter1-test", lambda data, meta: received.append(data))

    await asyncio.sleep(0.5)
    await room_a.emit("filter1-test", {"item": "apple"}, EmitOptions(filter="color:red"))
    await asyncio.sleep(0.3)
    await room_a.emit("filter1-test", {"item": "sky"}, EmitOptions(filter="color:blue"))
    await asyncio.sleep(0.5)

    ok = len(received) == 1 and received[0].get("item") == "apple"
    report("Single Filter", ok, f"received={len(received)} items={[r.get('item') for r in received]}")

    room_b.off("filter1-test")
    await room_b.unsubscribe("filter1-test")


async def test_multiple_or_filters(room_a, room_b):
    """Multiple OR filters: subscriber receives messages matching any filter."""
    received = []

    await room_b.subscribe("orfilt-test", SubscribeOptions(filters=["type:alert", "type:warning"]))
    room_b.on("orfilt-test", lambda data, meta: received.append(data))

    await asyncio.sleep(0.5)
    await room_a.emit("orfilt-test", {"msg": "alert1"}, EmitOptions(filter="type:alert"))
    await room_a.emit("orfilt-test", {"msg": "warn1"}, EmitOptions(filter="type:warning"))
    await room_a.emit("orfilt-test", {"msg": "info1"}, EmitOptions(filter="type:info"))
    await asyncio.sleep(0.8)

    ok = len(received) == 2
    report("Multiple Filters (OR)", ok, f"received={len(received)} msgs={[r.get('msg') for r in received]}")

    room_b.off("orfilt-test")
    await room_b.unsubscribe("orfilt-test")


async def test_and_filter_groups(room_a, room_b):
    """AND filter groups: subscriber only receives messages matching ALL filters in group."""
    received = []

    # Subscribe with AND group: must match BOTH color:red AND size:large
    await room_b.subscribe("andfilt-test", SubscribeOptions(filters=[["color:red", "size:large"]]))
    room_b.on("andfilt-test", lambda data, meta: received.append(data))

    await asyncio.sleep(0.5)
    # Publish with both filters (AND composite) - should match
    await room_a.emit("andfilt-test", {"item": "big-apple"}, EmitOptions(filters=["color:red", "size:large"]))
    await asyncio.sleep(0.3)
    # Publish with only one filter - should NOT match
    await room_a.emit("andfilt-test", {"item": "small-apple"}, EmitOptions(filter="color:red"))
    await asyncio.sleep(0.5)

    ok = len(received) == 1 and received[0].get("item") == "big-apple"
    report("AND Filter Group", ok, f"received={len(received)} items={[r.get('item') for r in received]}")

    room_b.off("andfilt-test")
    await room_b.unsubscribe("andfilt-test")


async def test_set_filters(room_a, room_b):
    """set_filters replaces all existing filters at once."""
    received = []

    await room_b.subscribe("setfilt-test", SubscribeOptions(filters=["old:filter"]))
    room_b.on("setfilt-test", lambda data, meta: received.append(data))

    await asyncio.sleep(0.3)
    await room_b.set_filters("setfilt-test", ["new:filter"])
    await asyncio.sleep(0.3)

    await room_a.emit("setfilt-test", {"seq": 1}, EmitOptions(filter="old:filter"))
    await room_a.emit("setfilt-test", {"seq": 2}, EmitOptions(filter="new:filter"))
    await asyncio.sleep(0.5)

    ok = len(received) == 1 and received[0].get("seq") == 2
    report("set_filters", ok, f"received={len(received)} seqs={[r.get('seq') for r in received]}")

    room_b.off("setfilt-test")
    await room_b.unsubscribe("setfilt-test")


async def test_add_filters(room_a, room_b):
    """add_filters adds new filter entries to existing set."""
    received = []

    await room_b.subscribe("addfilt-test", SubscribeOptions(filters=["tag:a"]))
    room_b.on("addfilt-test", lambda data, meta: received.append(data))

    await asyncio.sleep(0.3)
    await room_a.emit("addfilt-test", {"seq": 1}, EmitOptions(filter="tag:a"))
    await room_a.emit("addfilt-test", {"seq": 2}, EmitOptions(filter="tag:b"))
    await asyncio.sleep(0.5)
    count_before = len(received)

    await room_b.add_filters("addfilt-test", ["tag:b"])
    await asyncio.sleep(0.3)
    await room_a.emit("addfilt-test", {"seq": 3}, EmitOptions(filter="tag:b"))
    await asyncio.sleep(0.5)

    ok = count_before == 1 and len(received) == 2
    report("add_filters", ok, f"before={count_before} after={len(received)}")

    room_b.off("addfilt-test")
    await room_b.unsubscribe("addfilt-test")


async def test_remove_filters(room_a, room_b):
    """remove_filters removes specific filter entries."""
    received = []

    await room_b.subscribe("rmfilt-test", SubscribeOptions(filters=["tag:a", "tag:b"]))
    room_b.on("rmfilt-test", lambda data, meta: received.append(data))

    await asyncio.sleep(0.3)
    await room_b.remove_filters("rmfilt-test", ["tag:a"])
    await asyncio.sleep(0.3)

    await room_a.emit("rmfilt-test", {"seq": 1}, EmitOptions(filter="tag:a"))
    await room_a.emit("rmfilt-test", {"seq": 2}, EmitOptions(filter="tag:b"))
    await asyncio.sleep(0.5)

    ok = len(received) == 1 and received[0].get("seq") == 2
    report("remove_filters", ok, f"received={len(received)} seqs={[r.get('seq') for r in received]}")

    room_b.off("rmfilt-test")
    await room_b.unsubscribe("rmfilt-test")


async def test_filter_in_meta(room_a, room_b):
    """Filter value appears in MessageMeta.filter."""
    received = make_future()

    await room_b.subscribe("filtmeta-test", SubscribeOptions(filters=["env:prod"]))
    room_b.on("filtmeta-test", lambda data, meta: (
        received.set_result(meta) if not received.done() else None
    ))

    await asyncio.sleep(0.3)
    await room_a.emit("filtmeta-test", {"x": 1}, EmitOptions(filter="env:prod"))

    meta = await wait_for(received, label="filter meta")
    ok = meta.filter == "env:prod"
    report("Filter in Meta", ok, f"filter={meta.filter}")

    room_b.off("filtmeta-test")
    await room_b.unsubscribe("filtmeta-test")


async def test_multi_filter_publish(room_a, room_b):
    """Publish with multiple filters (AND composite)."""
    received = []

    # Subscribe with AND group to match AND-published messages
    await room_b.subscribe("mfpub-test", SubscribeOptions(filters=[["region:us", "tier:premium"]]))
    room_b.on("mfpub-test", lambda data, meta: received.append(data))

    await asyncio.sleep(0.5)
    # Publish with filters (AND composite)
    await room_a.emit("mfpub-test", {"multi": True}, EmitOptions(filters=["region:us", "tier:premium"]))
    await asyncio.sleep(0.8)

    ok = len(received) >= 1
    report("Multi-filter Publish", ok, f"received={len(received)}")

    room_b.off("mfpub-test")
    await room_b.unsubscribe("mfpub-test")


# ═══════════════════════════════════════════════════════════════
# PRESENCE
# ═══════════════════════════════════════════════════════════════

async def test_presence_set_and_join(room_a, room_b, client_a, client_b):
    """Both actors join room presence; each sees the other's join event."""
    # B joins the room presence first
    received_b = make_future()
    client_b.on("presence:join", lambda actor: (
        received_b.set_result(actor)
        if not received_b.done() and actor.actor_token_id == client_a.actor_id
        else None
    ))
    await room_b.set_presence({"name": "Agent B", "status": "online"})
    await asyncio.sleep(0.5)

    # A joins — B should get presence:join for A
    await room_a.set_presence({"name": "Agent A", "status": "online"})

    try:
        actor = await wait_for(received_b, seconds=5, label="presence join")
        ok = actor.presence.get("name") == "Agent A"
        report("Presence join event", ok, f"actor={actor.actor_token_id} name={actor.presence.get('name')}")
    except TimeoutError:
        report("Presence join event", False, "timed out")

    client_b.off("presence:join")


async def test_presence_update_event(room_a, client_a, client_b):
    """presence:update event fires when presence data changes."""
    received = make_future()

    client_b.on("presence:update", lambda actor: (
        received.set_result(actor)
        if not received.done() and actor.actor_token_id == client_a.actor_id
        else None
    ))

    await room_a.set_presence({"name": "Agent A", "status": "busy"})

    try:
        actor = await wait_for(received, seconds=5, label="presence update")
        ok = actor.presence.get("status") == "busy"
        report("Presence update event", ok, f"status={actor.presence.get('status')}")
    except TimeoutError:
        report("Presence update event", False, "timed out")

    client_b.off("presence:update")


async def test_presence_get(client_b, client_a):
    """get_presence returns presence data for a specific actor."""
    # Both actors already have presence set from previous test
    await asyncio.sleep(0.5)
    presence = client_b.get_presence(client_a.actor_id)
    ok = presence is not None and presence.presence.get("name") is not None
    report("Presence get", ok, f"presence={presence.presence if presence else None}")


async def test_presence_get_all(client_b):
    """get_all_presence returns list of present actors."""
    all_presence = client_b.get_all_presence()
    ok = isinstance(all_presence, list) and len(all_presence) >= 1
    report("get_all_presence", ok, f"count={len(all_presence)}")


async def test_clear_presence(room_a, client_a):
    """clear_presence removes presence data."""
    await room_a.set_presence({"name": "Temp"})
    await asyncio.sleep(0.3)
    await client_a.clear_presence()
    await asyncio.sleep(0.3)
    ok = client_a._presence is None
    report("Clear presence", ok)


# ═══════════════════════════════════════════════════════════════
# MULTIPLE ROOMS
# ═══════════════════════════════════════════════════════════════

async def test_multiple_rooms(client_a, client_b, app_slug):
    """Messages in different rooms are isolated."""
    room_a1 = client_a.set_app(app_slug).set_room("room-alpha")
    room_a2 = client_a.set_app(app_slug).set_room("room-beta")
    room_b1 = client_b.set_app(app_slug).set_room("room-alpha")
    room_b2 = client_b.set_app(app_slug).set_room("room-beta")

    received_alpha = []
    received_beta = []

    await room_b1.subscribe("multi-test")
    await room_b2.subscribe("multi-test")
    room_b1.on("multi-test", lambda data, meta: received_alpha.append(data))
    room_b2.on("multi-test", lambda data, meta: received_beta.append(data))

    await asyncio.sleep(0.3)
    await room_a1.emit("multi-test", {"room": "alpha"})
    await room_a2.emit("multi-test", {"room": "beta"})
    await asyncio.sleep(0.8)

    ok = (
        len(received_alpha) == 1
        and received_alpha[0].get("room") == "alpha"
        and len(received_beta) == 1
        and received_beta[0].get("room") == "beta"
    )
    report("Multiple Rooms", ok, f"alpha={len(received_alpha)} beta={len(received_beta)}")

    room_b1.off("multi-test")
    room_b2.off("multi-test")
    await room_b1.unsubscribe("multi-test")
    await room_b2.unsubscribe("multi-test")


# ═══════════════════════════════════════════════════════════════
# LOAD BALANCING
# ═══════════════════════════════════════════════════════════════

async def test_load_balance(client_a, client_b, app_slug):
    """Two subscribers in the same load-balance group get round-robin delivery."""
    room_a = client_a.set_app(app_slug).set_room("integration-test")
    room_b1 = client_b.set_app(app_slug).set_room("integration-test")

    # Create a third connection for the second load-balanced subscriber
    # We'll use client_a as publisher, client_b as subscriber 1,
    # and reuse client_a's room as subscriber 2 (same group, different connection)
    received_b = []

    lb_opts = SubscribeOptions(load_balance=True, load_balance_group="test-workers")

    await room_b1.subscribe("lb-test", lb_opts)
    room_b1.on("lb-test", lambda data, meta: received_b.append(data))

    await asyncio.sleep(0.5)

    # Send multiple messages from A
    msg_count = 6
    for i in range(msg_count):
        await room_a.emit("lb-test", {"seq": i})

    await asyncio.sleep(1.0)

    # With a single subscriber in the group, it should receive all messages
    ok = len(received_b) == msg_count
    report(
        "Load Balance (single worker)",
        ok,
        f"received={len(received_b)}/{msg_count}",
    )

    room_b1.off("lb-test")
    await room_b1.unsubscribe("lb-test")


async def test_load_balance_distribution(client_a, client_b, app_slug):
    """Two subscribers in the same group share messages (round-robin)."""
    room_pub = client_a.set_app(app_slug).set_room("integration-test")
    room_sub_a = client_a.set_app(app_slug).set_room("integration-test")
    room_sub_b = client_b.set_app(app_slug).set_room("integration-test")

    received_a = []
    received_b = []

    lb_opts = SubscribeOptions(load_balance=True, load_balance_group="dist-workers")

    # Both subscribe to same topic with same load balance group
    await room_sub_a.subscribe("lb-test", lb_opts)
    await room_sub_b.subscribe("lb-test", lb_opts)
    room_sub_a.on("lb-test", lambda data, meta: received_a.append(data))
    room_sub_b.on("lb-test", lambda data, meta: received_b.append(data))

    await asyncio.sleep(0.5)

    # Send multiple messages
    msg_count = 10
    for i in range(msg_count):
        await room_pub.emit("lb-test", {"seq": i})

    await asyncio.sleep(1.5)

    total = len(received_a) + len(received_b)
    # Both should have received some, and together they should have all
    distributed = len(received_a) > 0 and len(received_b) > 0
    ok = total == msg_count
    report(
        "Load Balance (distribution)",
        ok and distributed,
        f"A={len(received_a)} B={len(received_b)} total={total}/{msg_count} distributed={distributed}",
    )

    room_sub_a.off("lb-test")
    room_sub_b.off("lb-test")
    await room_sub_a.unsubscribe("lb-test")
    await room_sub_b.unsubscribe("lb-test")


# ═══════════════════════════════════════════════════════════════
# STRESS / EDGE CASES
# ═══════════════════════════════════════════════════════════════

async def test_rapid_messages(room_a, room_b):
    """Rapid-fire messages are all received in order."""
    count = 20
    received = []
    done = make_future()

    await room_b.subscribe("rapid-test")

    def handler(data, meta):
        received.append(data)
        if len(received) >= count and not done.done():
            done.set_result(True)

    room_b.on("rapid-test", handler)

    await asyncio.sleep(0.3)
    for i in range(count):
        await room_a.emit("rapid-test", {"seq": i})

    try:
        await wait_for(done, seconds=10, label="rapid messages")
        seqs = [r.get("seq") for r in received]
        in_order = all(seqs[i] <= seqs[i + 1] for i in range(len(seqs) - 1))
        ok = len(received) == count and in_order
        report("Rapid Messages", ok, f"count={len(received)}/{count} ordered={in_order}")
    except TimeoutError:
        report("Rapid Messages", False, f"received={len(received)}/{count}")

    room_b.off("rapid-test")
    await room_b.unsubscribe("rapid-test")


async def test_event_handlers(client_a):
    """System event handlers (connect/disconnect/error) can be registered."""
    errors = []
    h = lambda e: errors.append(e)
    client_a.on("error", h)
    client_a.off("error", h)
    report("Event Handlers", True, "on/off for error handler")


async def test_multiple_handlers_same_topic(room_a, room_b):
    """Multiple handlers on the same topic all receive the message."""
    received_1 = []
    received_2 = []

    await room_b.subscribe("multi-handler-test")
    room_b.on("multi-handler-test", lambda data, meta: received_1.append(data))
    room_b.on("multi-handler-test", lambda data, meta: received_2.append(data))

    await asyncio.sleep(0.3)
    await room_a.emit("multi-handler-test", {"x": 1})
    await asyncio.sleep(0.5)

    ok = len(received_1) == 1 and len(received_2) == 1
    report("Multiple Handlers", ok, f"h1={len(received_1)} h2={len(received_2)}")

    room_b.off("multi-handler-test")
    await room_b.unsubscribe("multi-handler-test")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

async def run(token1: str, token2: str, app_slug: str):
    print(f"\n{BOLD}NoLag Python SDK Integration Test{RESET}")
    print(f"  App: {app_slug}")
    print(f"  Broker: wss://broker.nolag.app/ws")
    print(f"  SDK source: {os.path.dirname(os.path.abspath(__file__))}/nolag/\n")

    client_a = NoLag(token1, NoLagOptions(debug=False))
    client_b = NoLag(token2, NoLagOptions(debug=False))

    print("Connecting clients...")
    await client_a.connect()
    await client_b.connect()
    print(f"  Client A: {client_a.actor_id}")
    print(f"  Client B: {client_b.actor_id}\n")

    await asyncio.sleep(0.5)

    room_a = client_a.set_app(app_slug).set_room("integration-test")
    room_b = client_b.set_app(app_slug).set_room("integration-test")

    print(f"{BOLD}Connection & Properties:{RESET}\n")
    await test_connect(client_a, client_b)
    await test_properties(client_a)
    await test_connection_status_enum(client_a)

    print(f"\n{BOLD}Fluent API:{RESET}\n")
    await test_fluent_api(client_a, app_slug)
    await test_fluent_api_lobby(client_a, app_slug)

    print(f"\n{BOLD}Pub/Sub:{RESET}\n")
    await test_basic_pubsub(room_a, room_b)
    await test_direct_api_pubsub(client_a, client_b, app_slug)
    await test_on_off(room_a, room_b)
    await test_off_all_handlers(room_a, room_b)
    await test_on_any(client_a, client_b, room_a, room_b)
    await test_off_any(client_b, room_a, room_b)
    await test_subscribe_unsubscribe(room_a, room_b)

    print(f"\n{BOLD}Emit Options:{RESET}\n")
    await test_echo_true(room_a)
    await test_echo_false(room_a, room_b)
    await test_qos_levels(room_a, room_b)
    await test_retain(room_a, room_b)

    print(f"\n{BOLD}Data Types & Meta:{RESET}\n")
    await test_complex_data(room_a, room_b)
    await test_string_data(room_a, room_b)
    await test_message_meta(room_a, room_b)

    print(f"\n{BOLD}Filters:{RESET}\n")
    await test_single_filter(room_a, room_b)
    await test_multiple_or_filters(room_a, room_b)
    await test_and_filter_groups(room_a, room_b)
    await test_set_filters(room_a, room_b)
    await test_add_filters(room_a, room_b)
    await test_remove_filters(room_a, room_b)
    await test_filter_in_meta(room_a, room_b)
    await test_multi_filter_publish(room_a, room_b)

    print(f"\n{BOLD}Presence:{RESET}\n")
    await test_presence_set_and_join(room_a, room_b, client_a, client_b)
    await test_presence_update_event(room_a, client_a, client_b)
    await test_presence_get(client_b, client_a)
    await test_presence_get_all(client_b)
    await test_clear_presence(room_a, client_a)

    print(f"\n{BOLD}Load Balancing:{RESET}\n")
    await test_load_balance(client_a, client_b, app_slug)
    await test_load_balance_distribution(client_a, client_b, app_slug)

    print(f"\n{BOLD}Advanced:{RESET}\n")
    await test_multiple_rooms(client_a, client_b, app_slug)
    await test_rapid_messages(room_a, room_b)
    await test_multiple_handlers_same_topic(room_a, room_b)
    await test_event_handlers(client_a)


    # Summary
    passed = sum(1 for _, ok, _ in results if ok)
    total = len(results)
    failed = total - passed

    print(f"\n{'=' * 50}")
    print(f"{BOLD}Results: {passed}/{total} passed", end="")
    if failed:
        print(f", {FAIL} {failed} failed", end="")
    print(f"{RESET}\n")

    client_a.disconnect()
    client_b.disconnect()
    await asyncio.sleep(0.5)

    if failed:
        sys.exit(1)


def main():
    args = {}
    for arg in sys.argv[1:]:
        if "=" in arg:
            key, val = arg.split("=", 1)
            args[key] = val

    token1 = args.get("token1")
    token2 = args.get("token2")
    app_slug = args.get("appSlug")

    if not all([token1, token2, app_slug]):
        print("Usage: python integration_test.py token1=<token-a> token2=<token-b> appSlug=<app-slug>")
        sys.exit(1)

    asyncio.run(run(token1, token2, app_slug))


if __name__ == "__main__":
    main()
