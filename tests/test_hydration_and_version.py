"""
Two guards added with 2.6.0.

The broker's hydration frame used to fall off the end of the dispatch chain
unhandled, so a hydration webhook's response never reached user code. And
`__version__` drifted from pyproject.toml between releases.
"""

from __future__ import annotations

import importlib.metadata
import tomllib
from pathlib import Path

import msgpack
import pytest

import nolag
from nolag import NoLag, PaginatedResult, Pagination


@pytest.mark.asyncio
async def test_hydration_frame_is_emitted_as_an_event():
    client = NoLag("test-token")
    seen = []
    client.on("hydration", lambda topic, data: seen.append((topic, data)))

    await client._handle_message(
        msgpack.packb({"type": "hydration", "topic": "messages", "data": {"items": [1, 2]}})
    )

    assert seen == [("messages", {"items": [1, 2]})]


def test_paginated_result_reads_the_pagination_envelope():
    result = PaginatedResult(
        data=[],
        pagination=Pagination.from_dict({"total": 42, "page": 2, "pageCount": 5}),
    )
    assert result.pagination.total == 42
    assert result.pagination.page == 2
    assert result.pagination.page_count == 5


def test_version_matches_pyproject():
    pyproject = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())
    assert nolag.__version__ == pyproject["project"]["version"]


def test_version_matches_installed_metadata_when_installed():
    try:
        installed = importlib.metadata.version("nolag")
    except importlib.metadata.PackageNotFoundError:
        pytest.skip("nolag is not installed in this environment")
    if installed != nolag.__version__:
        pytest.skip("an older nolag is installed alongside the checkout")
    assert installed == nolag.__version__
