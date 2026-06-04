"""
NoLag REST API Types

These types are for the project-scoped API.
API keys are scoped to a specific project, so organization and project IDs
are implicit and not needed in API calls.
"""

from dataclasses import dataclass, field
from typing import Any, Optional, Literal


# ============ Common Types ============

@dataclass
class PaginatedResult:
    """Paginated API response"""
    data: list
    total: int
    page: int
    limit: int
    total_pages: int


@dataclass
class ApiError:
    """API error response"""
    status_code: int
    message: str
    error: Optional[str] = None


# ============ App Types ============

@dataclass
class App:
    """App resource"""
    app_id: str
    project_id: str
    name: str
    slug: Optional[str] = None
    description: Optional[str] = None
    blueprint_id: Optional[str] = None
    config: Optional[dict[str, Any]] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    deleted_at: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> "App":
        return cls(
            app_id=data.get("appId", ""),
            project_id=data.get("projectId", ""),
            name=data.get("name", ""),
            slug=data.get("slug"),
            description=data.get("description"),
            blueprint_id=data.get("blueprintId"),
            config=data.get("config"),
            created_at=data.get("createdAt"),
            updated_at=data.get("updatedAt"),
            deleted_at=data.get("deletedAt"),
        )


@dataclass
class AppCreate:
    """Create app request"""
    name: str
    slug: Optional[str] = None
    description: Optional[str] = None
    blueprint_id: Optional[str] = None
    config: Optional[dict[str, Any]] = None

    def to_dict(self) -> dict:
        result = {"name": self.name}
        if self.slug:
            result["slug"] = self.slug
        if self.description:
            result["description"] = self.description
        if self.blueprint_id:
            result["blueprintId"] = self.blueprint_id
        if self.config:
            result["config"] = self.config
        return result


@dataclass
class AppUpdate:
    """Update app request"""
    name: Optional[str] = None
    slug: Optional[str] = None
    description: Optional[str] = None
    config: Optional[dict[str, Any]] = None

    def to_dict(self) -> dict:
        result = {}
        if self.name is not None:
            result["name"] = self.name
        if self.slug is not None:
            result["slug"] = self.slug
        if self.description is not None:
            result["description"] = self.description
        if self.config is not None:
            result["config"] = self.config
        return result


# ============ Room Types ============

RoomType = Literal["static", "dynamic"]


@dataclass
class Room:
    """Room resource"""
    room_id: str
    app_id: str
    name: str
    slug: str
    description: Optional[str] = None
    room_type: Optional[RoomType] = None
    is_enabled: bool = True
    topics: Optional[list[str]] = None
    config: Optional[dict[str, Any]] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> "Room":
        return cls(
            room_id=data.get("roomId", ""),
            app_id=data.get("appId", ""),
            name=data.get("name", ""),
            slug=data.get("slug", ""),
            description=data.get("description"),
            room_type=data.get("roomType"),
            is_enabled=data.get("isEnabled", True),
            topics=data.get("topics"),
            config=data.get("config"),
            created_at=data.get("createdAt"),
            updated_at=data.get("updatedAt"),
        )


@dataclass
class RoomCreate:
    """Create room request"""
    name: str
    slug: Optional[str] = None
    description: Optional[str] = None
    topics: Optional[list[str]] = None
    config: Optional[dict[str, Any]] = None

    def to_dict(self) -> dict:
        result = {"name": self.name}
        if self.slug:
            result["slug"] = self.slug
        if self.description:
            result["description"] = self.description
        if self.topics:
            result["topics"] = self.topics
        if self.config:
            result["config"] = self.config
        return result


@dataclass
class RoomUpdate:
    """Update room request"""
    name: Optional[str] = None
    description: Optional[str] = None
    is_enabled: Optional[bool] = None
    topics: Optional[list[str]] = None
    config: Optional[dict[str, Any]] = None

    def to_dict(self) -> dict:
        result = {}
        if self.name is not None:
            result["name"] = self.name
        if self.description is not None:
            result["description"] = self.description
        if self.is_enabled is not None:
            result["isEnabled"] = self.is_enabled
        if self.topics is not None:
            result["topics"] = self.topics
        if self.config is not None:
            result["config"] = self.config
        return result


# ============ Actor Types ============

ActorTokenType = Literal["device", "user", "server"]


@dataclass
class Actor:
    """Actor resource"""
    actor_token_id: str
    project_id: str
    name: str
    actor_type: ActorTokenType
    description: Optional[str] = None
    external_id: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None
    is_active: bool = True
    last_connected_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> "Actor":
        return cls(
            actor_token_id=data.get("actorTokenId", ""),
            project_id=data.get("projectId", ""),
            name=data.get("name", ""),
            actor_type=data.get("actorType", "device"),
            description=data.get("description"),
            external_id=data.get("externalId"),
            metadata=data.get("metadata"),
            is_active=data.get("isActive", True),
            last_connected_at=data.get("lastConnectedAt"),
            created_at=data.get("createdAt"),
            updated_at=data.get("updatedAt"),
        )


@dataclass
class ActorWithToken(Actor):
    """Actor with access token (returned on creation only)"""
    access_token: str = ""

    @classmethod
    def from_dict(cls, data: dict) -> "ActorWithToken":
        return cls(
            actor_token_id=data.get("actorTokenId", ""),
            project_id=data.get("projectId", ""),
            name=data.get("name", ""),
            actor_type=data.get("actorType", "device"),
            description=data.get("description"),
            external_id=data.get("externalId"),
            metadata=data.get("metadata"),
            is_active=data.get("isActive", True),
            last_connected_at=data.get("lastConnectedAt"),
            created_at=data.get("createdAt"),
            updated_at=data.get("updatedAt"),
            access_token=data.get("accessToken", ""),
        )


@dataclass
class ActorCreate:
    """Create actor request"""
    name: str
    actor_type: ActorTokenType
    description: Optional[str] = None
    external_id: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None

    def to_dict(self) -> dict:
        result = {
            "name": self.name,
            "actorType": self.actor_type,
        }
        if self.description:
            result["description"] = self.description
        if self.external_id:
            result["externalId"] = self.external_id
        if self.metadata:
            result["metadata"] = self.metadata
        return result


_UNSET = object()


@dataclass
class ActorUpdate:
    """Update actor request"""
    name: Optional[str] = None
    description: Optional[str] = None
    external_id: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None
    is_active: Optional[bool] = None
    access_scope_id: Any = _UNSET  # Use _UNSET sentinel so None means "unscope"

    def to_dict(self) -> dict:
        result = {}
        if self.name is not None:
            result["name"] = self.name
        if self.description is not None:
            result["description"] = self.description
        if self.external_id is not None:
            result["externalId"] = self.external_id
        if self.metadata is not None:
            result["metadata"] = self.metadata
        if self.is_active is not None:
            result["isActive"] = self.is_active
        if self.access_scope_id is not _UNSET:
            result["accessScopeId"] = self.access_scope_id
        return result


# ============ Scope Types ============


@dataclass
class Scope:
    """Access scope resource"""
    access_scope_id: str
    project_id: str
    slug: str
    name: str
    description: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None
    is_active: bool = True
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> "Scope":
        return cls(
            access_scope_id=data.get("accessScopeId", ""),
            project_id=data.get("projectId", ""),
            slug=data.get("slug", ""),
            name=data.get("name", ""),
            description=data.get("description"),
            metadata=data.get("metadata"),
            is_active=data.get("isActive", True),
            created_at=data.get("createdAt"),
            updated_at=data.get("updatedAt"),
        )


@dataclass
class ScopeCreate:
    """Create scope request"""
    slug: str
    name: str
    description: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None

    def to_dict(self) -> dict:
        result = {"slug": self.slug, "name": self.name}
        if self.description:
            result["description"] = self.description
        if self.metadata:
            result["metadata"] = self.metadata
        return result


@dataclass
class ScopeUpdate:
    """Update scope request"""
    name: Optional[str] = None
    description: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None
    is_active: Optional[bool] = None

    def to_dict(self) -> dict:
        result = {}
        if self.name is not None:
            result["name"] = self.name
        if self.description is not None:
            result["description"] = self.description
        if self.metadata is not None:
            result["metadata"] = self.metadata
        if self.is_active is not None:
            result["isActive"] = self.is_active
        return result


# ============ API Options ============

@dataclass
class NoLagApiOptions:
    """Options for the NoLag API client"""
    base_url: str = "https://api.nolag.app/v1"
    timeout: float = 30.0
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class ListOptions:
    """Options for list endpoints"""
    page: Optional[int] = None
    limit: Optional[int] = None
    sort_by: Optional[str] = None
    sort_order: Optional[Literal["asc", "desc"]] = None
    search: Optional[str] = None

    def to_params(self) -> dict:
        params = {}
        if self.page is not None:
            params["page"] = self.page
        if self.limit is not None:
            params["limit"] = self.limit
        if self.sort_by is not None:
            params["sortBy"] = self.sort_by
        if self.sort_order is not None:
            params["sortOrder"] = self.sort_order
        if self.search is not None:
            params["search"] = self.search
        return params
