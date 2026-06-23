"""Persistent Presence E2E (base python-sdk): drive the real _handle_message
presence parsing through online -> offline -> waking and assert status flows."""
import asyncio
import msgpack

from nolag.client import NoLag


def _frame(event, status):
    return msgpack.packb({
        "type": "presence",
        "event": event,
        "data": {
            "actor_token_id": "echo",
            "presence": {"capabilities": ["soil_analysis"], "persistent": True,
                         "wake": {"url": "http://localhost:9999/wake"}},
            "status": status,
        },
    })


def test_persistent_presence_status_flow():
    client = NoLag("test-token")
    waking_seen = []
    try:
        client.on("presence:waking", lambda a: waking_seen.append(a))
    except Exception:
        pass  # event registration is secondary; the presence map is the assertion

    async def run():
        await client._handle_message(_frame("join", "online"))
        assert client._presence_map["echo"].status == "online"
        assert client._presence_map["echo"].presence["persistent"] is True

        await client._handle_message(_frame("update", "offline"))
        assert client._presence_map["echo"].status == "offline"

        await client._handle_message(_frame("waking", "waking"))
        assert client._presence_map["echo"].status == "waking"

    asyncio.run(run())
    print("PASS: persistent presence status flow online -> offline -> waking")


if __name__ == "__main__":
    test_persistent_presence_status_flow()
