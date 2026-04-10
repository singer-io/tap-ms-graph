import unittest

from base import MS_GraphBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


@unittest.skip("Not applicable for full table streams: all MS Graph streams use FULL TABLE "
               "replication with no bookmark-based interruption/resumption logic.")
class MS_GraphInterruptedSyncTest(InterruptedSyncTest, MS_GraphBaseTest):
    """Test tap can recover from an interrupted sync.

    NOTE: This test is not applicable for full table streams. All MS Graph
    streams use FULL TABLE replication with no incremental bookmarks, so the
    interrupted sync resumption logic tested here does not apply.
    """

    @staticmethod
    def name():
        return "tap_tester_ms_graph_interrupted_sync_test"

    def streams_to_test(self):
        # Exclude streams with no records
        streams_to_exclude = {
            'audit_logs_signins', 'chats', 'conditional_access_policies',
            'drives', 'calendar_events', 'contacts', 'mail_messages',
            'chat_messages', 'drive_items', 'audit_logs_directory', 'teams',
            'channels', 'team_member'
        }
        return self.expected_stream_names().difference(streams_to_exclude)

    def manipulate_state(self):
        # All MS Graph streams are FULL TABLE and do not use bookmarks;
        # this state is only kept to satisfy the abstract method requirement.
        return {
            "currently_syncing": None,
            "bookmarks": {}
        }