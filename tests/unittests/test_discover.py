"""Unit tests for tap_ms_graph/discover.py"""
import pytest
from unittest.mock import MagicMock, patch
# Explicit submodule import so `tap_ms_graph.discover` resolves to the module
# object rather than the `discover` function aliased in tap_ms_graph/__init__.py.
import tap_ms_graph.discover  # noqa: F401
from tap_ms_graph.discover import discover
from tap_ms_graph.exceptions import MsGraphForbiddenError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MOCK_SCHEMA = {
    "type": "object",
    "properties": {"id": {"type": ["null", "string"]}},
}

MOCK_FIELD_METADATA = [
    {"breadcrumb": [], "metadata": {"table-key-properties": ["id"]}}
]


def _make_streams(config: dict):
    """Return a mock STREAMS dict.

    config: {stream_name: parent_or_empty_string}
    """
    result = {}
    for name, parent in config.items():
        cls = MagicMock()
        cls.parent = parent
        result[name] = cls
    return result


@pytest.fixture
def schemas_and_metadata():
    schemas = {
        "users": MOCK_SCHEMA,
        "groups": MOCK_SCHEMA,
        "group_member": MOCK_SCHEMA,
    }
    field_metadata = {
        "users": MOCK_FIELD_METADATA,
        "groups": MOCK_FIELD_METADATA,
        "group_member": MOCK_FIELD_METADATA,
    }
    return schemas, field_metadata


# ---------------------------------------------------------------------------
# Tests: discover() without a client (no permission check)
# ---------------------------------------------------------------------------

class TestDiscoverWithoutClient:
    def test_returns_all_streams(self, schemas_and_metadata):
        schemas, field_metadata = schemas_and_metadata
        mock_streams = _make_streams(
            {"users": "", "groups": "", "group_member": "groups"}
        )

        with patch("tap_ms_graph.discover.get_schemas", return_value=(schemas, field_metadata)), \
             patch("tap_ms_graph.discover.STREAMS", mock_streams):
            catalog = discover(client=None)

        stream_names = {e.stream for e in catalog.streams}
        assert stream_names == {"users", "groups", "group_member"}

    def test_returns_catalog_instance(self, schemas_and_metadata):
        from singer.catalog import Catalog
        schemas, field_metadata = schemas_and_metadata
        mock_streams = _make_streams({"users": ""})

        with patch("tap_ms_graph.discover.get_schemas", return_value=(schemas, field_metadata)), \
             patch("tap_ms_graph.discover.STREAMS", mock_streams):
            catalog = discover(client=None)

        assert isinstance(catalog, Catalog)

    def test_catalog_entries_have_key_properties(self, schemas_and_metadata):
        schemas, field_metadata = schemas_and_metadata
        mock_streams = _make_streams({"users": ""})

        with patch("tap_ms_graph.discover.get_schemas", return_value=(schemas, field_metadata)), \
             patch("tap_ms_graph.discover.STREAMS", mock_streams):
            catalog = discover(client=None)

        assert catalog.streams[0].key_properties == ["id"]

    def test_no_check_access_called_without_client(self, schemas_and_metadata):
        schemas, field_metadata = schemas_and_metadata
        mock_streams = _make_streams({"users": ""})

        with patch("tap_ms_graph.discover.get_schemas", return_value=(schemas, field_metadata)), \
             patch("tap_ms_graph.discover.STREAMS", mock_streams):
            discover(client=None)

        mock_streams["users"].check_access.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: discover() with a client (permission checks active)
# ---------------------------------------------------------------------------

class TestDiscoverWithClient:
    def test_all_accessible_streams_included(self, schemas_and_metadata):
        schemas, field_metadata = schemas_and_metadata
        mock_client = MagicMock()
        mock_streams = _make_streams(
            {"users": "", "groups": "", "group_member": "groups"}
        )
        for cls in mock_streams.values():
            cls.check_access.return_value = None

        with patch("tap_ms_graph.discover.get_schemas", return_value=(schemas, field_metadata)), \
             patch("tap_ms_graph.discover.STREAMS", mock_streams):
            catalog = discover(client=mock_client)

        stream_names = {e.stream for e in catalog.streams}
        assert stream_names == {"users", "groups", "group_member"}

    def test_forbidden_top_level_stream_excluded(self, schemas_and_metadata):
        schemas, field_metadata = schemas_and_metadata
        mock_client = MagicMock()
        mock_streams = _make_streams({"users": "", "groups": ""})
        mock_streams["users"].check_access.return_value = None
        mock_streams["groups"].check_access.side_effect = MsGraphForbiddenError("403")

        with patch("tap_ms_graph.discover.get_schemas", return_value=(schemas, field_metadata)), \
             patch("tap_ms_graph.discover.STREAMS", mock_streams):
            catalog = discover(client=mock_client)

        stream_names = {e.stream for e in catalog.streams}
        assert "groups" not in stream_names
        assert "users" in stream_names

    def test_all_forbidden_raises_error(self, schemas_and_metadata):
        schemas, field_metadata = schemas_and_metadata
        mock_client = MagicMock()
        mock_streams = _make_streams({"users": "", "groups": ""})
        for cls in mock_streams.values():
            cls.check_access.side_effect = MsGraphForbiddenError("403")

        with patch("tap_ms_graph.discover.get_schemas", return_value=(schemas, field_metadata)), \
             patch("tap_ms_graph.discover.STREAMS", mock_streams):
            catalog = discover(client=mock_client)

        # All streams inaccessible → catalog is empty (no raise)
        assert catalog.streams == []

    def test_child_excluded_when_parent_inaccessible(self, schemas_and_metadata):
        schemas, field_metadata = schemas_and_metadata
        mock_client = MagicMock()
        mock_streams = _make_streams(
            {"users": "", "groups": "", "group_member": "groups"}
        )
        mock_streams["users"].check_access.return_value = None
        mock_streams["groups"].check_access.side_effect = MsGraphForbiddenError("403")

        with patch("tap_ms_graph.discover.get_schemas", return_value=(schemas, field_metadata)), \
             patch("tap_ms_graph.discover.STREAMS", mock_streams):
            catalog = discover(client=mock_client)

        stream_names = {e.stream for e in catalog.streams}
        assert "group_member" not in stream_names
        assert "users" in stream_names

    def test_child_included_when_parent_accessible(self, schemas_and_metadata):
        schemas, field_metadata = schemas_and_metadata
        mock_client = MagicMock()
        mock_streams = _make_streams(
            {"users": "", "groups": "", "group_member": "groups"}
        )
        mock_streams["users"].check_access.return_value = None
        mock_streams["groups"].check_access.return_value = None

        with patch("tap_ms_graph.discover.get_schemas", return_value=(schemas, field_metadata)), \
             patch("tap_ms_graph.discover.STREAMS", mock_streams):
            catalog = discover(client=mock_client)

        stream_names = {e.stream for e in catalog.streams}
        assert "group_member" in stream_names

    def test_check_access_not_called_for_child_streams(self, schemas_and_metadata):
        schemas, field_metadata = schemas_and_metadata
        mock_client = MagicMock()
        mock_streams = _make_streams({"groups": "", "group_member": "groups"})
        mock_streams["groups"].check_access.return_value = None

        with patch("tap_ms_graph.discover.get_schemas", return_value=(schemas, field_metadata)), \
             patch("tap_ms_graph.discover.STREAMS", mock_streams):
            discover(client=mock_client)

        # Only top-level streams get check_access called
        mock_streams["groups"].check_access.assert_called_once_with(mock_client)
        mock_streams["group_member"].check_access.assert_not_called()

    def test_multiple_children_excluded_with_parent(self, schemas_and_metadata):
        """All children of a forbidden parent stream are excluded."""
        extra_schemas = {
            **schemas_and_metadata[0],
            "group_owner": MOCK_SCHEMA,
        }
        extra_metadata = {
            **schemas_and_metadata[1],
            "group_owner": MOCK_FIELD_METADATA,
        }
        mock_client = MagicMock()
        mock_streams = _make_streams(
            {"groups": "", "group_member": "groups", "group_owner": "groups"}
        )
        mock_streams["groups"].check_access.side_effect = MsGraphForbiddenError("403")

        with patch("tap_ms_graph.discover.get_schemas", return_value=(extra_schemas, extra_metadata)), \
             patch("tap_ms_graph.discover.STREAMS", mock_streams):
            # All top-level streams forbidden → empty catalog
            catalog = discover(client=mock_client)

        assert catalog.streams == []

    def test_catalog_entries_have_tap_stream_id(self, schemas_and_metadata):
        schemas, field_metadata = schemas_and_metadata
        mock_client = MagicMock()
        mock_streams = _make_streams({"users": ""})
        mock_streams["users"].check_access.return_value = None

        with patch("tap_ms_graph.discover.get_schemas", return_value=(schemas, field_metadata)), \
             patch("tap_ms_graph.discover.STREAMS", mock_streams):
            catalog = discover(client=mock_client)

        assert catalog.streams[0].tap_stream_id == "users"
