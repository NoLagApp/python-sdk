"""Handler collections must tolerate mutation during dispatch.

Handlers are stored in sets. A handler is allowed to call on(), off(), or tear
down a wrapper while it runs, all of which mutate the set being iterated. Before
these were snapshotted, that raised "Set changed size during iteration" and the
remaining handlers were skipped. nolag-signal used to monkey-patch the client to
work around it.
"""

from __future__ import annotations

import msgpack

from nolag import NoLag
from nolag.types import MessageMeta


def _client() -> NoLag:
    return NoLag("test-token")


class TestEventHandlerMutation:
    def test_handler_can_remove_itself_during_dispatch(self):
        client = _client()
        calls = []

        def handler(*args):
            calls.append("self")
            client.off("connect", handler)

        client.on("connect", handler)
        client._emit_event("connect")

        assert calls == ["self"]
        assert client._event_handlers["connect"] == set()

    def test_handler_can_remove_a_sibling_during_dispatch(self):
        """The detach case: one wrapper releasing handlers while another runs."""
        client = _client()
        calls = []

        def second(*args):
            calls.append("second")

        def first(*args):
            calls.append("first")
            client.off("connect", second)

        client.on("connect", first)
        client.on("connect", second)

        client._emit_event("connect")

        # first always runs; second may or may not, depending on set order, but
        # the dispatch must not raise and must not lose first.
        assert "first" in calls
        assert second not in client._event_handlers["connect"]

    def test_handler_can_register_a_new_handler_during_dispatch(self):
        client = _client()
        calls = []

        def added(*args):
            calls.append("added")

        def adder(*args):
            calls.append("adder")
            client.on("connect", added)

        client.on("connect", adder)
        client._emit_event("connect")

        assert "adder" in calls
        assert added in client._event_handlers["connect"]

    def test_all_handlers_run_when_one_mutates(self):
        client = _client()
        calls = []

        def mutator(*args):
            calls.append("mutator")
            client.off("connect", mutator)

        def a(*args):
            calls.append("a")

        def b(*args):
            calls.append("b")

        client.on("connect", a)
        client.on("connect", mutator)
        client.on("connect", b)

        client._emit_event("connect")

        # Every handler present at dispatch time must have run.
        assert sorted(calls) == ["a", "b", "mutator"]


class TestMessageHandlerMutation:
    @staticmethod
    def _frame(topic: str) -> bytes:
        # _handle_message takes msgpack bytes off the wire, not a dict.
        return msgpack.packb({"type": "message", "topic": topic, "data": {"x": 1}})

    async def test_topic_handler_can_unsubscribe_itself_during_dispatch(self):
        client = _client()
        calls = []

        def handler(data, meta: MessageMeta):
            calls.append(data)
            client.off("app/room/topic", handler)

        client._message_handlers.setdefault("app/room/topic", set()).add(handler)
        await client._handle_message(self._frame("app/room/topic"))

        assert calls == [{"x": 1}]
        assert client._message_handlers["app/room/topic"] == set()

    async def test_any_handler_can_remove_itself_during_dispatch(self):
        client = _client()
        calls = []

        def any_handler(topic, data, meta):
            calls.append(topic)
            client.off_any(any_handler)

        client.on_any(any_handler)
        await client._handle_message(self._frame("app/room/topic"))

        assert calls == ["app/room/topic"]
        assert client._any_handlers == set()
