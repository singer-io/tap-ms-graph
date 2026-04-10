"""Unit tests for tap_ms_graph/sync.py"""
import pytest
from unittest.mock import MagicMock, patch

import singer

from tap_ms_graph.sync import update_currently_syncing, write_schema, sync
from tap_ms_graph.exceptions import MsGraphBackoffError, MsGraphRateLimitError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_mock_stream(parent="", children=None, sync_return=0):
    """Create a pre-configured stream mock."""
    stream = MagicMock()
    stream.parent = parent
    stream.children = children or []
    stream.child_to_sync = []
    stream.is_selected.return_value = True
    stream.sync.return_value = sync_return
    return stream


def make_mock_catalog(stream_names):
    """Return a catalog mock whose get_selected_streams yields entries."""
    catalog = MagicMock()
    entries = [MagicMock(stream=name) for name in stream_names]
    catalog.get_selected_streams.return_value = entries
    catalog.get_stream.return_value = MagicMock()
    return catalog


def _run_sync(streams_map, selected_streams, state=None):
    """Helper: call sync() with patched STREAMS and singer.Transformer."""
    client = MagicMock()
    config = {"start_date": "2024-01-01T00:00:00Z"}
    catalog = make_mock_catalog(selected_streams)
    state = state or {}

    with patch("tap_ms_graph.sync.STREAMS", streams_map), \
         patch("tap_ms_graph.sync.write_schema"), \
         patch("singer.get_currently_syncing", return_value=None), \
         patch("singer.set_currently_syncing"), \
         patch("singer.write_state"):
        mock_tx = MagicMock()
        with patch("singer.Transformer") as mock_cls:
            mock_cls.return_value.__enter__ = MagicMock(return_value=mock_tx)
            mock_cls.return_value.__exit__ = MagicMock(return_value=False)
            sync(client=client, config=config, catalog=catalog, state=state)

    return client, catalog, state


# ---------------------------------------------------------------------------
# Tests: update_currently_syncing
# ---------------------------------------------------------------------------

class TestUpdateCurrentlySyncing:
    @patch("singer.write_state")
    @patch("singer.set_currently_syncing")
    @patch("singer.get_currently_syncing", return_value=None)
    def test_sets_current_stream(self, mock_get, mock_set, mock_write):
        state = {}
        update_currently_syncing(state, "users")
        mock_set.assert_called_once_with(state, "users")
        mock_write.assert_called_once_with(state)

    @patch("singer.write_state")
    @patch("singer.set_currently_syncing")
    @patch("singer.get_currently_syncing", return_value="users")
    def test_clears_currently_syncing_when_none(self, mock_get, mock_set, mock_write):
        state = {"currently_syncing": "users"}
        update_currently_syncing(state, None)
        assert "currently_syncing" not in state
        mock_write.assert_called_once_with(state)

    @patch("singer.write_state")
    @patch("singer.set_currently_syncing")
    @patch("singer.get_currently_syncing", return_value=None)
    def test_no_error_when_not_currently_syncing(self, mock_get, mock_set, mock_write):
        state = {}
        # Calling with None when nothing is set should not raise
        update_currently_syncing(state, None)
        mock_write.assert_called_once_with(state)


# ---------------------------------------------------------------------------
# Tests: write_schema
# ---------------------------------------------------------------------------

class TestWriteSchema:
    def test_write_schema_called_when_selected(self):
        stream = make_mock_stream()
        stream.is_selected.return_value = True
        write_schema(stream, MagicMock(), [], MagicMock())
        stream.write_schema.assert_called_once()

    def test_write_schema_not_called_when_not_selected(self):
        stream = make_mock_stream()
        stream.is_selected.return_value = False
        write_schema(stream, MagicMock(), [], MagicMock())
        stream.write_schema.assert_not_called()

    def test_child_in_streams_to_sync_added_to_child_list(self):
        """Children present in streams_to_sync are appended to stream.child_to_sync."""
        stream = make_mock_stream(children=["group_member"])
        mock_child_instance = make_mock_stream(parent="groups")

        mock_catalog_entry = MagicMock()
        mock_catalog_entry.schema.to_dict.return_value = {
            "type": "object", "properties": {}
        }
        mock_catalog_entry.metadata = []
        catalog = MagicMock()
        catalog.get_stream.return_value = mock_catalog_entry

        child_cls = MagicMock(return_value=mock_child_instance)

        with patch.dict("tap_ms_graph.sync.STREAMS", {"group_member": child_cls}):
            write_schema(stream, MagicMock(), ["group_member"], catalog)

        assert mock_child_instance in stream.child_to_sync

    def test_child_not_in_streams_to_sync_not_added(self):
        """Children absent from streams_to_sync are NOT added to child_to_sync."""
        stream = make_mock_stream(children=["group_member"])
        mock_child_instance = make_mock_stream(parent="groups")

        mock_catalog_entry = MagicMock()
        mock_catalog_entry.schema.to_dict.return_value = {
            "type": "object", "properties": {}
        }
        mock_catalog_entry.metadata = []
        catalog = MagicMock()
        catalog.get_stream.return_value = mock_catalog_entry

        child_cls = MagicMock(return_value=mock_child_instance)

        with patch.dict("tap_ms_graph.sync.STREAMS", {"group_member": child_cls}):
            # "group_member" not in streams_to_sync
            write_schema(stream, MagicMock(), [], catalog)

        assert mock_child_instance not in stream.child_to_sync


# ---------------------------------------------------------------------------
# Tests: sync()
# ---------------------------------------------------------------------------

class TestSync:
    def test_top_level_stream_is_synced(self):
        mock_users = make_mock_stream(sync_return=3)
        streams_map = {"users": MagicMock(return_value=mock_users)}
        _run_sync(streams_map, ["users"])
        mock_users.sync.assert_called_once()

    def test_child_stream_is_skipped_directly(self):
        """A child stream selected alone is skipped (sync not called on it)."""
        mock_group_member = make_mock_stream(parent="groups")
        mock_groups = make_mock_stream(sync_return=0)
        streams_map = {
            "group_member": MagicMock(return_value=mock_group_member),
            "groups": MagicMock(return_value=mock_groups),
        }
        _run_sync(streams_map, ["group_member"])
        # group_member.sync should not be called directly; the parent gets synced
        mock_group_member.sync.assert_not_called()

    def test_parent_added_when_child_selected(self):
        """Selecting a child stream causes the parent to be synced."""
        mock_group_member = make_mock_stream(parent="groups")
        mock_groups = make_mock_stream(sync_return=0)
        streams_map = {
            "group_member": MagicMock(return_value=mock_group_member),
            "groups": MagicMock(return_value=mock_groups),
        }
        _run_sync(streams_map, ["group_member"])
        mock_groups.sync.assert_called_once()

    def test_continues_on_backoff_error(self):
        """After MsGraphBackoffError, remaining streams still sync."""
        mock_users = make_mock_stream()
        mock_users.sync.side_effect = MsGraphBackoffError("server error")

        mock_groups = make_mock_stream(sync_return=5)
        streams_map = {
            "users": MagicMock(return_value=mock_users),
            "groups": MagicMock(return_value=mock_groups),
        }
        _run_sync(streams_map, ["users", "groups"])
        mock_users.sync.assert_called_once()
        mock_groups.sync.assert_called_once()

    def test_continues_on_rate_limit_error(self):
        """After MsGraphRateLimitError, remaining streams still sync."""
        mock_users = make_mock_stream()
        mock_users.sync.side_effect = MsGraphRateLimitError("rate limit")

        mock_groups = make_mock_stream(sync_return=2)
        streams_map = {
            "users": MagicMock(return_value=mock_users),
            "groups": MagicMock(return_value=mock_groups),
        }
        _run_sync(streams_map, ["users", "groups"])
        mock_groups.sync.assert_called_once()

    @patch("singer.write_state")
    @patch("singer.set_currently_syncing")
    @patch("singer.get_currently_syncing", return_value=None)
    def test_updates_currently_syncing_before_and_after(
        self, mock_get, mock_set, mock_write
    ):
        """State is set to the stream name before sync and cleared after."""
        mock_users = make_mock_stream(sync_return=1)
        streams_map = {"users": MagicMock(return_value=mock_users)}

        client = MagicMock()
        catalog = make_mock_catalog(["users"])
        state = {}

        with patch("tap_ms_graph.sync.STREAMS", streams_map), \
             patch("tap_ms_graph.sync.write_schema"):
            mock_tx = MagicMock()
            with patch("singer.Transformer") as mock_cls:
                mock_cls.return_value.__enter__ = MagicMock(return_value=mock_tx)
                mock_cls.return_value.__exit__ = MagicMock(return_value=False)
                sync(client=client, config={"start_date": "2024-01-01"},
                     catalog=catalog, state=state)

        # set_currently_syncing called with "users" then None
        calls = mock_set.call_args_list
        stream_values = [c[0][1] for c in calls]
        assert "users" in stream_values
        assert None in stream_values

    def test_multiple_streams_all_synced(self):
        """All selected top-level streams are synced."""
        mock_users = make_mock_stream(sync_return=3)
        mock_groups = make_mock_stream(sync_return=7)
        mock_apps = make_mock_stream(sync_return=2)
        streams_map = {
            "users": MagicMock(return_value=mock_users),
            "groups": MagicMock(return_value=mock_groups),
            "applications": MagicMock(return_value=mock_apps),
        }
        _run_sync(streams_map, ["users", "groups", "applications"])
        mock_users.sync.assert_called_once()
        mock_groups.sync.assert_called_once()
        mock_apps.sync.assert_called_once()

    def test_backoff_error_does_not_stop_subsequent_streams(self):
        """First stream fails; second and third still run."""
        mock_s1 = make_mock_stream()
        mock_s1.sync.side_effect = MsGraphBackoffError("error")
        mock_s2 = make_mock_stream(sync_return=4)
        mock_s3 = make_mock_stream(sync_return=6)
        streams_map = {
            "s1": MagicMock(return_value=mock_s1),
            "s2": MagicMock(return_value=mock_s2),
            "s3": MagicMock(return_value=mock_s3),
        }
        _run_sync(streams_map, ["s1", "s2", "s3"])
        mock_s2.sync.assert_called_once()
        mock_s3.sync.assert_called_once()
