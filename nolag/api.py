"""
NoLag REST API Client

Provides management operations for NoLag resources via REST API.
API keys are scoped to a specific project, so no organization or project IDs needed.

Use this for managing apps, rooms, and actors within your project.
For real-time messaging, use the main NoLag WebSocket client.
"""

import aiohttp
from typing import Any, Optional
from urllib.parse import urlencode

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

DEFAULT_BASE_URL = "https://api.nolag.app/v1"
DEFAULT_TIMEOUT = 30.0


class NoLagApiError(Exception):
    """NoLag API Error"""

    def __init__(self, message: str, status_code: int, details: Optional[ApiError] = None):
        super().__init__(message)
        self.status_code = status_code
        self.details = details or ApiError(status_code=status_code, message=message)


class AppsApi:
    """Apps API - Manage apps in your project"""

    def __init__(self, api: "NoLagApi"):
        self._api = api

    async def list(self, options: Optional[ListOptions] = None) -> PaginatedResult:
        """List all apps in the project"""
        params = options.to_params() if options else {}
        data = await self._api._request("GET", "/apps", params=params)
        return PaginatedResult(
            data=[App.from_dict(item) for item in data.get("data", [])],
            total=data.get("total", 0),
            page=data.get("page", 1),
            limit=data.get("limit", 10),
            total_pages=data.get("totalPages", 1),
        )

    async def get(self, app_id: str) -> App:
        """Get an app by ID"""
        data = await self._api._request("GET", f"/apps/{app_id}")
        return App.from_dict(data)

    async def create(self, data: AppCreate) -> App:
        """Create a new app"""
        result = await self._api._request("POST", "/apps", json=data.to_dict())
        return App.from_dict(result)

    async def update(self, app_id: str, data: AppUpdate) -> App:
        """Update an app"""
        result = await self._api._request("PATCH", f"/apps/{app_id}", json=data.to_dict())
        return App.from_dict(result)

    async def delete(self, app_id: str) -> dict:
        """Delete an app (soft delete)"""
        return await self._api._request("DELETE", f"/apps/{app_id}")

    async def reset_to_blueprint(self, app_id: str) -> App:
        """Reset app to its blueprint configuration"""
        result = await self._api._request("POST", f"/apps/{app_id}/reset-to-blueprint")
        return App.from_dict(result)


class RoomsApi:
    """Rooms API - Manage rooms within apps"""

    def __init__(self, api: "NoLagApi"):
        self._api = api

    async def list(self, app_id: str) -> list[Room]:
        """List all rooms in an app"""
        data = await self._api._request("GET", f"/apps/{app_id}/rooms")
        return [Room.from_dict(item) for item in data]

    async def get(self, app_id: str, room_id: str) -> Room:
        """Get a room by ID"""
        data = await self._api._request("GET", f"/apps/{app_id}/rooms/{room_id}")
        return Room.from_dict(data)

    async def create(self, app_id: str, data: RoomCreate) -> Room:
        """Create a new dynamic room"""
        result = await self._api._request("POST", f"/apps/{app_id}/rooms", json=data.to_dict())
        return Room.from_dict(result)

    async def update(self, app_id: str, room_id: str, data: RoomUpdate) -> Room:
        """Update a room"""
        result = await self._api._request(
            "PATCH", f"/apps/{app_id}/rooms/{room_id}", json=data.to_dict()
        )
        return Room.from_dict(result)

    async def delete(self, app_id: str, room_id: str) -> None:
        """Delete a dynamic room (static rooms cannot be deleted)"""
        await self._api._request("DELETE", f"/apps/{app_id}/rooms/{room_id}")


class ActorsApi:
    """Actors API - Manage actors in your project"""

    def __init__(self, api: "NoLagApi"):
        self._api = api

    async def list(self) -> list[Actor]:
        """List all actors in the project"""
        data = await self._api._request("GET", "/actors")
        return [Actor.from_dict(item) for item in data]

    async def get(self, actor_id: str) -> Actor:
        """Get an actor by ID"""
        data = await self._api._request("GET", f"/actors/{actor_id}")
        return Actor.from_dict(data)

    async def create(self, data: ActorCreate) -> ActorWithToken:
        """
        Create a new actor

        IMPORTANT: The access token is only returned once! Save it immediately.
        """
        result = await self._api._request("POST", "/actors", json=data.to_dict())
        return ActorWithToken.from_dict(result)

    async def update(self, actor_id: str, data: ActorUpdate) -> Actor:
        """Update an actor"""
        result = await self._api._request("PATCH", f"/actors/{actor_id}", json=data.to_dict())
        return Actor.from_dict(result)

    async def delete(self, actor_id: str) -> None:
        """Delete an actor"""
        await self._api._request("DELETE", f"/actors/{actor_id}")


class ScopesApi:
    """Scopes API - Manage access scopes in your project"""

    def __init__(self, api: "NoLagApi"):
        self._api = api

    async def list(self, options: Optional[ListOptions] = None) -> PaginatedResult:
        """List all access scopes in the project"""
        params = options.to_params() if options else {}
        data = await self._api._request("GET", "/scopes", params=params)
        return PaginatedResult(
            data=[Scope.from_dict(item) for item in data.get("data", [])],
            total=data.get("total", 0),
            page=data.get("page", 1),
            limit=data.get("limit", 10),
            total_pages=data.get("totalPages", 1),
        )

    async def get(self, scope_id: str) -> Scope:
        """Get an access scope by ID"""
        data = await self._api._request("GET", f"/scopes/{scope_id}")
        return Scope.from_dict(data)

    async def create(self, data: ScopeCreate) -> Scope:
        """Create a new access scope"""
        result = await self._api._request("POST", "/scopes", json=data.to_dict())
        return Scope.from_dict(result)

    async def update(self, scope_id: str, data: ScopeUpdate) -> Scope:
        """Update an access scope"""
        result = await self._api._request("PATCH", f"/scopes/{scope_id}", json=data.to_dict())
        return Scope.from_dict(result)

    async def delete(self, scope_id: str) -> None:
        """Delete an access scope"""
        await self._api._request("DELETE", f"/scopes/{scope_id}")

    async def list_actors(self, scope_id: str) -> list[Actor]:
        """List actors assigned to a scope"""
        data = await self._api._request("GET", f"/scopes/{scope_id}/actors")
        return [Actor.from_dict(item) for item in data]

    async def add_actor(self, scope_id: str, actor_id: str) -> Actor:
        """Assign an actor to this scope"""
        return await self._api.actors.update(actor_id, ActorUpdate(access_scope_id=scope_id))

    async def remove_actor(self, actor_id: str) -> Actor:
        """Remove an actor from its scope (unscope it)"""
        return await self._api.actors.update(actor_id, ActorUpdate(access_scope_id=None))


class NoLagApi:
    """
    NoLag REST API Client

    API keys are project-scoped, so you don't need to pass organization or project IDs.

    Example:
        ```python
        from nolag import NoLagApi, AppCreate, ActorCreate

        async with NoLagApi('nlg_live_xxx.secret') as api:
            # List apps in your project
            apps = await api.apps.list()

            # Create a room
            room = await api.rooms.create(app_id, RoomCreate(
                name='chat-room',
                description='General chat'
            ))

            # Create an actor and get the access token
            actor = await api.actors.create(ActorCreate(
                name='web-client',
                actor_type='device'
            ))
            print('Save this token:', actor.access_token)
        ```
    """

    def __init__(
        self,
        api_key: str,
        options: Optional[NoLagApiOptions] = None,
    ):
        self._api_key = api_key
        self._options = options or NoLagApiOptions()
        self._base_url = self._options.base_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=self._options.timeout)
        self._session: Optional[aiohttp.ClientSession] = None

        # Initialize sub-APIs
        self.apps = AppsApi(self)
        self.rooms = RoomsApi(self)
        self.actors = ActorsApi(self)
        self.scopes = ScopesApi(self)

    async def __aenter__(self) -> "NoLagApi":
        """Async context manager entry"""
        self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit"""
        if self._session:
            await self._session.close()
            self._session = None

    async def close(self) -> None:
        """Close the API client session"""
        if self._session:
            await self._session.close()
            self._session = None

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[dict] = None,
        json: Optional[dict] = None,
    ) -> Any:
        """Make an authenticated request to the NoLag API"""
        if not self._session:
            self._session = aiohttp.ClientSession(timeout=self._timeout)

        url = f"{self._base_url}{path}"
        if params:
            url = f"{url}?{urlencode(params)}"

        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            **self._options.headers,
        }

        try:
            async with self._session.request(
                method, url, headers=headers, json=json
            ) as response:
                if not response.ok:
                    try:
                        error_data = await response.json()
                        raise NoLagApiError(
                            error_data.get("message", "Request failed"),
                            response.status,
                            ApiError(
                                status_code=response.status,
                                message=error_data.get("message", "Request failed"),
                                error=error_data.get("error"),
                            ),
                        )
                    except aiohttp.ContentTypeError:
                        raise NoLagApiError(
                            response.reason or "Request failed",
                            response.status,
                        )

                # Handle 204 No Content
                if response.status == 204:
                    return None

                return await response.json()

        except aiohttp.ClientError as e:
            raise NoLagApiError(str(e), 0, ApiError(status_code=0, message=str(e)))
