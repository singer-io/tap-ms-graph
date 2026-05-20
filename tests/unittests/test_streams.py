"""Unit tests for tap_ms_graph/streams/ (abstracts, check_access, get_records, sync)."""
import pytest
from unittest.mock import MagicMock, patch, call

from singer import metadata as singer_metadata

from tap_ms_graph.streams.abstracts import FullTableStream
from tap_ms_graph.streams.users import Users
from tap_ms_graph.streams.groups import Groups
from tap_ms_graph.streams.group_member import GroupMember
from tap_ms_graph.exceptions import (
    MsGraphForbiddenError,
    MsGraphBadRequestError,
    MsGraphBackoffError,
    MsGraphInternalServerError,
    MsGraphNotFoundError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SIMPLE_SCHEMA = {
    "type": "object",
    "properties": {"id": {"type": ["null", "string"]}},
}


def make_client(base_url="https://graph.microsoft.com/v1.0"):
    client = MagicMock()
    client.base_url = base_url
    client.config = {"page_size": 10, "start_date": "2024-01-01T00:00:00Z"}
    return client


def make_catalog_entry(schema_dict=None, key_properties=None, selected=True):
    """Build a catalog entry whose metadata works with singer.metadata helpers."""
    schema_dict = schema_dict or SIMPLE_SCHEMA
    key_properties = key_properties or ["id"]

    mdata = singer_metadata.get_standard_metadata(
        schema=schema_dict,
        key_properties=key_properties,
        valid_replication_keys=[],
        replication_method="FULL_TABLE",
    )
    mdata_map = singer_metadata.to_map(mdata)
    mdata_map = singer_metadata.write(mdata_map, (), "selected", selected)
    mdata_list = singer_metadata.to_list(mdata_map)

    entry = MagicMock()
    entry.schema.to_dict.return_value = schema_dict
    entry.metadata = mdata_list
    return entry





# ---------------------------------------------------------------------------
# Tests: BaseStream.check_access
# ---------------------------------------------------------------------------

class TestCheckAccess:
    def test_top_level_success_no_exception(self):
        client = make_client()
        client.get.return_value = {"value": []}
        # Should not raise
        Users.check_access(client)

    def test_get_called_once(self):
        client = make_client()
        client.get.return_value = {"value": []}
        Users.check_access(client)
        client.get.assert_called_once()

    def test_uses_correct_endpoint(self):
        client = make_client()
        client.get.return_value = {"value": []}
        Users.check_access(client)
        endpoint_used = client.get.call_args[0][0]
        assert endpoint_used == f"{client.base_url}/{Users.path}"

    def test_passes_top_1_param(self):
        client = make_client()
        client.get.return_value = {"value": []}
        Users.check_access(client)
        params = client.get.call_args[0][1]
        assert params == {"$top": "1"}

    def test_raises_forbidden_on_403(self):
        client = make_client()
        client.get.side_effect = MsGraphForbiddenError("403 Forbidden")
        with pytest.raises(MsGraphForbiddenError):
            Users.check_access(client)

    def test_converts_bad_request_to_forbidden(self):
        """A 400 is re-raised as-is; the caller (discover.py) decides how to handle it."""
        client = make_client()
        client.get.side_effect = MsGraphBadRequestError("400 no license")
        with pytest.raises(MsGraphBadRequestError):
            Users.check_access(client)

    def test_converts_5xx_to_forbidden(self):
        """5xx errors are re-raised as-is; the caller (discover.py) decides how to handle them."""
        client = make_client()
        client.get.side_effect = MsGraphInternalServerError("500 server error")
        with pytest.raises(MsGraphInternalServerError):
            Users.check_access(client)

    def test_converts_generic_backoff_to_forbidden(self):
        """Backoff errors are re-raised as-is; the caller (discover.py) decides how to handle them."""
        client = make_client()
        client.get.side_effect = MsGraphBackoffError("backoff error")
        with pytest.raises(MsGraphBackoffError):
            Users.check_access(client)

    def test_child_stream_skips_api_call(self):
        """Child streams (with a parent) must not call the API during check_access."""
        client = make_client()
        GroupMember.check_access(client)
        client.get.assert_not_called()

    def test_child_stream_returns_none(self):
        client = make_client()
        result = GroupMember.check_access(client)
        assert result is None

    def test_groups_stream_uses_own_path(self):
        client = make_client()
        client.get.return_value = {"value": []}
        Groups.check_access(client)
        endpoint_used = client.get.call_args[0][0]
        assert endpoint_used == f"{client.base_url}/{Groups.path}"


# ---------------------------------------------------------------------------
# Tests: BaseStream.get_records (pagination)
# ---------------------------------------------------------------------------

class TestGetRecords:
    def test_single_page_returns_all_records(self):
        client = make_client()
        entry = make_catalog_entry()
        client.get.return_value = {"value": [{"id": "1"}, {"id": "2"}]}

        stream = Users(client, entry)
        stream.update_params()
        records = list(stream.get_records())

        assert len(records) == 2
        assert records[0]["id"] == "1"
        assert records[1]["id"] == "2"

    def test_pagination_fetches_multiple_pages(self):
        client = make_client()
        entry = make_catalog_entry()
        next_link = "https://graph.microsoft.com/v1.0/users?$skiptoken=abc"
        client.get.side_effect = [
            {"value": [{"id": "1"}], "@odata.nextLink": next_link},
            {"value": [{"id": "2"}]},
        ]

        stream = Users(client, entry)
        stream.update_params()
        records = list(stream.get_records())

        assert len(records) == 2
        assert client.get.call_count == 2

    def test_second_page_uses_next_link_url(self):
        """The nextLink URL replaces the endpoint URL for subsequent requests."""
        next_link = "https://graph.microsoft.com/v1.0/users?$skiptoken=xyz"
        client = make_client()
        entry = make_catalog_entry()
        client.get.side_effect = [
            {"value": [{"id": "1"}], "@odata.nextLink": next_link},
            {"value": [{"id": "2"}]},
        ]

        stream = Users(client, entry)
        stream.update_params()
        list(stream.get_records())

        second_call = client.get.call_args_list[1]
        assert second_call[0][0] == next_link

    def test_second_page_passes_empty_params(self):
        """nextLink already contains all params; we should not duplicate them."""
        next_link = "https://graph.microsoft.com/v1.0/users?$skiptoken=xyz"
        client = make_client()
        entry = make_catalog_entry()
        client.get.side_effect = [
            {"value": [{"id": "1"}], "@odata.nextLink": next_link},
            {"value": [{"id": "2"}]},
        ]

        stream = Users(client, entry)
        stream.update_params()
        list(stream.get_records())

        second_call = client.get.call_args_list[1]
        assert second_call[0][1] == {}

    def test_empty_response_yields_nothing(self):
        client = make_client()
        entry = make_catalog_entry()
        client.get.return_value = {"value": []}

        stream = Users(client, entry)
        stream.update_params()
        records = list(stream.get_records())

        assert records == []

    def test_three_pages(self):
        """Records accumulate across many pages."""
        next_link1 = "https://graph.microsoft.com/v1.0/users?$skip=1"
        next_link2 = "https://graph.microsoft.com/v1.0/users?$skip=2"
        client = make_client()
        entry = make_catalog_entry()
        client.get.side_effect = [
            {"value": [{"id": "1"}], "@odata.nextLink": next_link1},
            {"value": [{"id": "2"}], "@odata.nextLink": next_link2},
            {"value": [{"id": "3"}]},
        ]

        stream = Users(client, entry)
        stream.update_params()
        records = list(stream.get_records())

        assert len(records) == 3
        assert client.get.call_count == 3

    def test_not_found_404_yields_nothing_and_no_exception(self):
        """A 404 MsGraphNotFoundError means the resource does not exist for
        this parent (e.g. a user with no OneDrive).  get_records() must return
        an empty iterator rather than propagating the exception."""
        client = make_client()
        entry = make_catalog_entry()
        client.get.side_effect = MsGraphNotFoundError("HTTP-error-code: 404, Error: User's mysite not found.")

        stream = Users(client, entry)
        stream.update_params()
        records = list(stream.get_records())

        assert records == []
        client.get.assert_called_once()  # no retry after 404


# ---------------------------------------------------------------------------
# Tests: BaseStream.update_params
# ---------------------------------------------------------------------------

class TestUpdateParams:
    def test_update_params_sets_top(self):
        client = make_client()
        entry = make_catalog_entry()
        stream = Users(client, entry)
        stream.update_params()
        assert stream.params["$top"] == stream.page_size

    def test_update_params_merges_extra(self):
        client = make_client()
        entry = make_catalog_entry()
        stream = Users(client, entry)
        stream.update_params(updated_since="2024-01-01")
        assert stream.params["updated_since"] == "2024-01-01"
        assert "$top" in stream.params

    def test_page_size_from_config(self):
        client = make_client()
        client.config = {"page_size": 50, "start_date": "2024-01-01T00:00:00Z"}
        entry = make_catalog_entry()
        stream = Users(client, entry)
        stream.update_params()
        assert stream.params["$top"] == 50


# ---------------------------------------------------------------------------
# Tests: BaseStream.get_url_endpoint
# ---------------------------------------------------------------------------

class TestGetUrlEndpoint:
    def test_returns_base_url_plus_path_when_no_url_endpoint(self):
        client = make_client()
        entry = make_catalog_entry()
        stream = Users(client, entry)
        stream.url_endpoint = ""  # reset
        endpoint = stream.get_url_endpoint()
        assert endpoint == f"{client.base_url}/{Users.path}"

    def test_returns_url_endpoint_when_set(self):
        client = make_client()
        entry = make_catalog_entry()
        stream = Users(client, entry)
        stream.url_endpoint = "https://custom.endpoint/users"
        endpoint = stream.get_url_endpoint()
        assert endpoint == "https://custom.endpoint/users"


# ---------------------------------------------------------------------------
# Tests: FullTableStream.sync
# ---------------------------------------------------------------------------

class TestFullTableSync:
    @patch("tap_ms_graph.streams.abstracts.write_record")
    def test_writes_records_when_selected(self, mock_write_record):
        client = make_client()
        entry = make_catalog_entry(selected=True)
        stream = Users(client, entry)
        client.get.return_value = {"value": [{"id": "1"}, {"id": "2"}]}

        from singer import Transformer
        with Transformer() as transformer:
            count = stream.sync(state={}, transformer=transformer)

        assert count == 2
        assert mock_write_record.call_count == 2

    @patch("tap_ms_graph.streams.abstracts.write_record")
    def test_does_not_write_when_not_selected(self, mock_write_record):
        client = make_client()
        entry = make_catalog_entry(selected=False)
        stream = Users(client, entry)
        client.get.return_value = {"value": [{"id": "1"}]}

        from singer import Transformer
        with Transformer() as transformer:
            count = stream.sync(state={}, transformer=transformer)

        mock_write_record.assert_not_called()
        assert count == 0

    @patch("tap_ms_graph.streams.abstracts.write_record")
    def test_returns_correct_count(self, mock_write_record):
        client = make_client()
        entry = make_catalog_entry(selected=True)
        stream = Users(client, entry)
        records = [{"id": str(i)} for i in range(5)]
        client.get.return_value = {"value": records}

        from singer import Transformer
        with Transformer() as transformer:
            count = stream.sync(state={}, transformer=transformer)

        assert count == 5

    @patch("tap_ms_graph.streams.abstracts.write_record")
    def test_empty_result_returns_zero(self, mock_write_record):
        client = make_client()
        entry = make_catalog_entry(selected=True)
        stream = Users(client, entry)
        client.get.return_value = {"value": []}

        from singer import Transformer
        with Transformer() as transformer:
            count = stream.sync(state={}, transformer=transformer)

        assert count == 0
        mock_write_record.assert_not_called()

    @patch("tap_ms_graph.streams.abstracts.write_record")
    def test_syncs_children(self, mock_write_record):
        """Children in child_to_sync have their sync() called for each parent record."""
        client = make_client()
        entry = make_catalog_entry(selected=True)
        stream = Users(client, entry)
        client.get.return_value = {"value": [{"id": "u1"}, {"id": "u2"}]}

        mock_child = MagicMock()
        mock_child.sync.return_value = 0
        stream.child_to_sync = [mock_child]

        from singer import Transformer
        with Transformer() as transformer:
            stream.sync(state={}, transformer=transformer)

        assert mock_child.sync.call_count == 2


# ---------------------------------------------------------------------------
# Tests: FullTableStream.sync — additional cases
# ---------------------------------------------------------------------------

class TestFullTableSyncAdditional:
    @patch("tap_ms_graph.streams.abstracts.write_record")
    def test_sync_across_multiple_pages(self, mock_write_record):
        """Records fetched across two pages are all written."""
        next_link = "https://graph.microsoft.com/v1.0/users?$skiptoken=page2"
        client = make_client()
        entry = make_catalog_entry(selected=True)
        client.get.side_effect = [
            {"value": [{"id": "1"}, {"id": "2"}], "@odata.nextLink": next_link},
            {"value": [{"id": "3"}]},
        ]

        stream = Users(client, entry)
        from singer import Transformer
        with Transformer() as transformer:
            count = stream.sync(state={}, transformer=transformer)

        assert count == 3
        assert mock_write_record.call_count == 3

    @patch("tap_ms_graph.streams.abstracts.write_record")
    def test_sync_groups_stream(self, mock_write_record):
        """Groups (a different top-level stream) syncs correctly."""
        client = make_client()
        entry = make_catalog_entry(selected=True)
        client.get.return_value = {
            "value": [{"id": "g1"}, {"id": "g2"}, {"id": "g3"}]
        }

        stream = Groups(client, entry)
        from singer import Transformer
        with Transformer() as transformer:
            count = stream.sync(state={}, transformer=transformer)

        assert count == 3
        assert mock_write_record.call_count == 3

    @patch("tap_ms_graph.streams.abstracts.write_record")
    def test_sync_passes_parent_obj_to_children(self, mock_write_record):
        """Each record is passed as parent_obj to every attached child stream."""
        client = make_client()
        entry = make_catalog_entry(selected=True)
        records = [{"id": "u1"}, {"id": "u2"}, {"id": "u3"}]
        client.get.return_value = {"value": records}

        stream = Users(client, entry)
        mock_child = MagicMock()
        mock_child.sync.return_value = 0
        stream.child_to_sync = [mock_child]

        from singer import Transformer
        with Transformer() as transformer:
            stream.sync(state={}, transformer=transformer)

        # child.sync called once per parent record, with the correct parent_obj
        assert mock_child.sync.call_count == 3
        passed_parents = [
            call_args.kwargs["parent_obj"]["id"]
            for call_args in mock_child.sync.call_args_list
        ]
        assert passed_parents == ["u1", "u2", "u3"]

    @patch("tap_ms_graph.streams.abstracts.write_record")
    def test_sync_multiple_children_each_called_per_record(self, mock_write_record):
        """All children are called for every parent record."""
        client = make_client()
        entry = make_catalog_entry(selected=True)
        client.get.return_value = {"value": [{"id": "u1"}, {"id": "u2"}]}

        stream = Users(client, entry)
        child_a = MagicMock()
        child_a.sync.return_value = 0
        child_b = MagicMock()
        child_b.sync.return_value = 0
        stream.child_to_sync = [child_a, child_b]

        from singer import Transformer
        with Transformer() as transformer:
            stream.sync(state={}, transformer=transformer)

        assert child_a.sync.call_count == 2
        assert child_b.sync.call_count == 2

    @patch("tap_ms_graph.streams.abstracts.write_record")
    def test_sync_not_selected_children_still_called(self, mock_write_record):
        """Children in child_to_sync are always called regardless of parent selection."""
        client = make_client()
        entry = make_catalog_entry(selected=False)  # parent not selected
        client.get.return_value = {"value": [{"id": "u1"}]}

        stream = Users(client, entry)
        mock_child = MagicMock()
        mock_child.sync.return_value = 0
        stream.child_to_sync = [mock_child]

        from singer import Transformer
        with Transformer() as transformer:
            stream.sync(state={}, transformer=transformer)

        # Parent not selected → no parent records written, but child still called
        mock_write_record.assert_not_called()
        mock_child.sync.assert_called_once()

    @patch("tap_ms_graph.streams.abstracts.write_record")
    def test_sync_three_pages_total_count(self, mock_write_record):
        """Count accumulates correctly across three pages."""
        link1 = "https://graph.microsoft.com/v1.0/users?$skip=1"
        link2 = "https://graph.microsoft.com/v1.0/users?$skip=2"
        client = make_client()
        entry = make_catalog_entry(selected=True)
        client.get.side_effect = [
            {"value": [{"id": "1"}, {"id": "2"}], "@odata.nextLink": link1},
            {"value": [{"id": "3"}, {"id": "4"}], "@odata.nextLink": link2},
            {"value": [{"id": "5"}]},
        ]

        stream = Users(client, entry)
        from singer import Transformer
        with Transformer() as transformer:
            count = stream.sync(state={}, transformer=transformer)

        assert count == 5
        assert client.get.call_count == 3


# ---------------------------------------------------------------------------
# Tests: GroupMember (child stream with parent-dependent URL)
# ---------------------------------------------------------------------------

class TestChildStreamUrlEndpoint:
    def test_group_member_builds_url_with_parent_id(self):
        client = make_client()
        entry = make_catalog_entry(key_properties=["id", "group_id"])
        stream = GroupMember(client, entry)
        endpoint = stream.get_url_endpoint(parent_obj={"id": "group-abc"})
        assert "group-abc" in endpoint
        assert "members" in endpoint

    def test_group_member_raises_without_parent_obj(self):
        client = make_client()
        entry = make_catalog_entry(key_properties=["id", "group_id"])
        stream = GroupMember(client, entry)
        with pytest.raises(ValueError):
            stream.get_url_endpoint(parent_obj=None)

    def test_group_member_modifies_adds_group_id(self):
        client = make_client()
        entry = make_catalog_entry(key_properties=["id", "group_id"])
        stream = GroupMember(client, entry)
        record = {"id": "member-1"}
        modified = stream.modify_object(record, parent_record={"id": "grp-99"})
        assert modified["group_id"] == "grp-99"
