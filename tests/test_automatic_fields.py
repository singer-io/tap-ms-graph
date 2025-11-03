"""Test that with no fields selected for a stream automatic fields are still
replicated."""
from base import MS_GraphBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class MS_GraphAutomaticFields(MinimumSelectionTest, MS_GraphBaseTest):
    """Test that with no fields selected for a stream automatic fields are
    still replicated."""

    @staticmethod
    def name():
        return "tap_tester_ms_graph_automatic_fields_test"

    def streams_to_test(self):
        # Due to insufficient permissions on the API's
        streams_to_exclude = {'audit_logs_signins', 'chats', 'conditional_access_policies', 'drives', 'users', 'calendar_events', 'contacts', 'mail_messages', 'chat_messages', 'drive_items', 'audit_logs_directory', 'teams', 'channels','team_member', 'group_member', 'group_owner'}
        return self.expected_stream_names().difference(streams_to_exclude)
