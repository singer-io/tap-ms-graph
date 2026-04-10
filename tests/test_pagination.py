from tap_tester.base_suite_tests.pagination_test import PaginationTest
from base import MS_GraphBaseTest

class MS_GraphPaginationTest(PaginationTest, MS_GraphBaseTest):
    """
    Ensure tap can replicate multiple pages of data for streams that use pagination.
    """

    @staticmethod
    def name():
        return "tap_tester_ms_graph_pagination_test"

    def streams_to_test(self):
        # Exclude streams with no records
        streams_to_exclude = {
            'audit_logs_signins', 'chats', 'conditional_access_policies',
            'drives', 'calendar_events', 'contacts', 'mail_messages',
            'chat_messages', 'drive_items', 'audit_logs_directory', 'teams',
            'channels', 'team_member'
        }
        return self.expected_stream_names().difference(streams_to_exclude)
