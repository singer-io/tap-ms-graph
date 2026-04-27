import unittest

from base import MS_GraphBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


@unittest.skip("Not applicable for full table streams: all MS Graph streams use FULL TABLE "
               "replication and do not maintain incremental bookmarks.")
class MS_GraphBookMarkTest(BookmarkTest, MS_GraphBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream.

    NOTE: This test is not applicable for full table streams. All MS Graph
    streams use FULL TABLE replication and do not maintain incremental
    bookmarks, so bookmark-based assertions in this test class are not
    relevant.
    """
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    # No initial bookmarks needed - all streams are FULL TABLE with no replication keys
    initial_bookmarks = {}

    @staticmethod
    def name():
        return "tap_tester_ms_graph_bookmark_test"

    def streams_to_test(self):
        # Exclude streams with no records
        streams_to_exclude = {
            'audit_logs_signins', 'chats', 'conditional_access_policies',
            'drives', 'calendar_events', 'contacts', 'mail_messages',
            'chat_messages', 'drive_items', 'audit_logs_directory', 'teams',
            'channels', 'team_member'
        }
        return self.expected_stream_names().difference(streams_to_exclude)
