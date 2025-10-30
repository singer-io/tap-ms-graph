"""Test tap discovery mode and metadata."""
from base import MS_GraphBaseTest
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest


class MS_GraphDiscoveryTest(DiscoveryTest, MS_GraphBaseTest):
    """Test tap discovery mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_ms_graph_discovery_test"

    def streams_to_test(self):
        return self.expected_stream_names()