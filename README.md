# NoLag Python SDK

Real-time messaging SDK for Python applications.

## Installation

```bash
pip install nolag
```

## Quick Start

```python
import asyncio
from nolag import NoLag, NoLagOptions

async def main():
    # Create client with your actor token
    client = NoLag("your-actor-token")

    # Connect to NoLag
    await client.connect()

    # Subscribe to a topic
    def on_message(data, meta):
        print(f"Received: {data}")

    await client.subscribe("my-topic", on_message)

    # Publish a message
    await client.emit("my-topic", {"hello": "world"})

    # Keep running
    await asyncio.sleep(60)

    # Disconnect when done
    await client.disconnect()

asyncio.run(main())
```

## Configuration

```python
from nolag import NoLag, NoLagOptions, QoS

options = NoLagOptions(
    url="wss://broker.nolag.app/ws",  # Custom broker URL
    reconnect=True,                     # Auto-reconnect on disconnect
    reconnect_interval=5.0,             # Seconds between reconnect attempts
    max_reconnect_attempts=10,          # Max reconnect attempts (0 = infinite)
    heartbeat_interval=30.0,            # Heartbeat interval (0 to disable)
    qos=QoS.AT_LEAST_ONCE,              # Default QoS level
    debug=True,                         # Enable debug logging
)

client = NoLag("your-actor-token", options)
```

## Subscribing to Topics

```python
from nolag import SubscribeOptions, QoS

# Basic subscription
await client.subscribe("chat/messages", lambda data, meta: print(data))

# With options
options = SubscribeOptions(
    qos=QoS.EXACTLY_ONCE,
    load_balance=True,
    load_balance_group="workers"
)
await client.subscribe("tasks", handler, options)

# Unsubscribe
await client.unsubscribe("chat/messages")
```

## Publishing Messages

```python
from nolag import EmitOptions, QoS

# Publish any data (dict, list, string, bytes, etc.)
await client.emit("chat/messages", {"text": "Hello!"})

# With options
options = EmitOptions(
    qos=QoS.EXACTLY_ONCE,
    retain=True  # Retain last message for new subscribers
)
await client.emit("status", {"online": True}, options)
```

## Connection Events

```python
from nolag import ConnectionStatus

# Listen for connection events
client.on("connected", lambda: print("Connected!"))
client.on("disconnected", lambda: print("Disconnected"))
client.on("reconnecting", lambda attempt: print(f"Reconnecting... attempt {attempt}"))
client.on("error", lambda err: print(f"Error: {err}"))

# Check connection status
if client.status == ConnectionStatus.CONNECTED:
    print("We're connected!")
```

## Presence

```python
# Set your presence data
await client.set_presence({"status": "online", "typing": False})

# Get presence of all actors in a topic
presence_list = await client.get_presence("chat/room-1")
for actor in presence_list:
    print(f"{actor.actor_token_id}: {actor.presence}")

# Listen for presence changes
client.on("presence", lambda topic, presence:
    print(f"Presence update in {topic}: {presence}")
)
```

## Error Handling

```python
import asyncio
from nolag import NoLag

async def main():
    client = NoLag("your-actor-token")

    try:
        await client.connect()
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    # Handle errors during operation
    client.on("error", lambda err: print(f"Error: {err}"))

    try:
        await client.emit("topic", {"data": "value"})
    except Exception as e:
        print(f"Emit failed: {e}")

asyncio.run(main())
```

## QoS Levels

| Level | Name | Description |
|-------|------|-------------|
| 0 | AT_MOST_ONCE | Fire and forget, no acknowledgment |
| 1 | AT_LEAST_ONCE | Guaranteed delivery, may have duplicates |
| 2 | EXACTLY_ONCE | Guaranteed exactly one delivery |

```python
from nolag import QoS

# Set default QoS in options
options = NoLagOptions(qos=QoS.EXACTLY_ONCE)

# Or per-message
await client.emit("important", data, EmitOptions(qos=QoS.EXACTLY_ONCE))
```

## Load Balancing

Distribute messages across multiple subscribers:

```python
# Enable load balancing for a subscription
await client.subscribe(
    "tasks",
    process_task,
    SubscribeOptions(
        load_balance=True,
        load_balance_group="task-workers"
    )
)
```

## Type Definitions

```python
from nolag import (
    NoLag,              # Main client class
    NoLagOptions,       # Connection options
    SubscribeOptions,   # Subscription options
    EmitOptions,        # Publish options
    ConnectionStatus,   # Connection status enum
    ActorType,          # Actor type enum
    QoS,                # QoS level enum
    MessageMeta,        # Message metadata
    ActorPresence,      # Presence info
)
```

## REST API Client

The SDK also includes a REST API client for managing apps, rooms, and actors.

```python
import asyncio
from nolag import NoLagApi, AppCreate, RoomCreate, ActorCreate

async def main():
    # Create API client with project-scoped API key
    async with NoLagApi("nlg_live_xxx.secret") as api:
        # List all apps in your project
        apps = await api.apps.list()
        print(f"Found {len(apps.data)} apps")

        # Create a new app
        app = await api.apps.create(AppCreate(
            name="my-chat-app",
            description="A real-time chat application"
        ))
        print(f"Created app: {app.app_id}")

        # Create a room in the app
        room = await api.rooms.create(app.app_id, RoomCreate(
            name="general",
            slug="general",
            description="General chat room"
        ))
        print(f"Created room: {room.room_id}")

        # Create an actor (IMPORTANT: save the access token!)
        actor = await api.actors.create(ActorCreate(
            name="web-client",
            actor_type="device"
        ))
        print(f"Actor token (save this!): {actor.access_token}")

        # Update an app
        updated = await api.apps.update(app.app_id, AppUpdate(
            description="Updated description"
        ))

        # Delete resources
        await api.rooms.delete(app.app_id, room.room_id)
        await api.actors.delete(actor.actor_token_id)
        await api.apps.delete(app.app_id)

asyncio.run(main())
```

### API Types

```python
from nolag import (
    # API Client
    NoLagApi,           # REST API client
    NoLagApiError,      # API error class
    NoLagApiOptions,    # API client options

    # Resources
    App, AppCreate, AppUpdate,
    Room, RoomCreate, RoomUpdate,
    Actor, ActorWithToken, ActorCreate, ActorUpdate,

    # Utilities
    ListOptions,        # Pagination options
    PaginatedResult,    # Paginated response
)
```

## Requirements

- Python 3.10+
- websockets >= 12.0
- msgpack >= 1.0.0
- aiohttp >= 3.9.0

## License

MIT
