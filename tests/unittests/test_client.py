import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
import requests
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError
from freezegun import freeze_time

from tap_ms_graph.client import Client, raise_for_error
from tap_ms_graph.exceptions import (
    ERROR_CODE_EXCEPTION_MAPPING,
    MsGraphError,
    MsGraphUnauthorizedError,
    MsGraphBadRequestError,
    MsGraphRateLimitError,
    MsGraphInternalServerError,
)


class MockResponse:
    def __init__(self, status_code, json_data=None, raise_error=False, headers=None, text=None):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.raise_error = raise_error
        self.headers = headers or {}
        self.text = text or ""

    def raise_for_status(self):
        if self.raise_error:
            raise requests.HTTPError("Sample message")
        return self.status_code

    def json(self):
        return self._json_data


def get_response(status_code, json_data=None, headers=None, raise_error=False):
    """Helper function to create a mocked HTTP response."""
    return MockResponse(status_code, json_data, raise_error, headers)


@pytest.mark.parametrize(
    "status_code",
    [200, 201, 204],
    ids=["OK_200", "Created_201", "NoContent_204"]
)
def test_raise_for_error_success(status_code):
    """Test that raise_for_error does not raise exceptions on successful HTTP status codes."""
    response = get_response(status_code, {"message": "OK"})
    raise_for_error(response)  # Should not raise


@pytest.mark.parametrize(
    "response_data, expected_exception, expected_msg_part",
    [
        ({"code": "BadRequest", "details": "Invalid input"},
         MsGraphBadRequestError,
         "HTTP-error-code: 400"),
        ({"message": "Unauthorized access"},
         MsGraphUnauthorizedError,
         "Unauthorized access"),
        (None,
         MsGraphInternalServerError,
         "HTTP-error-code: 500"),
        ({"message": "Unknown error received from the API."},
         MsGraphError,
         "Unknown error"),
    ],
    ids=[
        "BadRequest_400",
        "Unauthorized_401",
        "InternalServerError_500",
        "GenericMsGraphError_418"
    ]
)
def test_raise_for_error_exceptions(response_data, expected_exception, expected_msg_part):
    """
    Test that raise_for_error raises the correct exception type with appropriate messages
    for different HTTP error responses.
    """
    code_map = {
        MsGraphBadRequestError: 400,
        MsGraphUnauthorizedError: 401,
        MsGraphInternalServerError: 500,
        MsGraphError: 418,
    }
    status_code = code_map[expected_exception]
    # For the generic error test, patch ERROR_CODE_EXCEPTION_MAPPING temporarily
    if status_code == 418:
        with patch('tap_ms_graph.exceptions.ERROR_CODE_EXCEPTION_MAPPING', new={}):
            response = get_response(status_code, response_data, raise_error=True)
            with pytest.raises(expected_exception) as excinfo:
                raise_for_error(response)
    else:
        response = get_response(status_code, response_data, raise_error=True)
        with pytest.raises(expected_exception) as excinfo:
            raise_for_error(response)

    assert expected_msg_part in str(excinfo.value)


@pytest.fixture
def client_config():
    """Fixture providing mock client configuration parameters."""
    return {
        "client_id": "mocked_client_id",
        "client_secret": "mocked_secret",
        "tenant_id": "mocked_tenant",
        "scope": "mocked_scope"
    }


@pytest.fixture
def mock_token():
    """Fixture returning a mocked successful token response."""
    return get_response(
        200,
        {
            "access_token": "mocked_token",
            "expires_in": 3600
        }
    )


class TestClientRequests:
    base_url = "https://graph.microsoft.com/v1.0"

    @pytest.fixture(autouse=True)
    def set_expected_headers(self):
        """Fixture that sets default expected headers for requests."""
        self.default_headers = {
            'User-Agent': 'singer',
            'Content-Type': 'application/json',
        }

    @pytest.mark.parametrize(
        "endpoint, params, headers, expected_response",
        [
            ("/me/messages", {}, {}, {"result": []}),
            ("/me/events", {"$top": 2}, {"X-Test": "1"}, {"value": ["event1", "event2"]}),
            ("/me/contacts", {"$filter": "emailAddress eq 'abc@test.com'"}, {}, {"value": ["contact1"]}),
        ],
        ids=["messages_no_params", "events_top_2_with_header", "contacts_filtered"]
    )
    @patch("requests.Session.request")
    def test_successful_request_multiple_cases(self, mock_request, client_config, mock_token,
                                               endpoint, params, headers, expected_response):
        """
        Test multiple successful GET request scenarios with varying endpoints,
        parameters, headers, and verify correct responses and request calls.
        """
        full_url = f"{self.base_url}{endpoint}"
        merged_headers = {**self.default_headers, **headers}
        expected_headers = dict(merged_headers)
        expected_headers["Authorization"] = "mocked_token"

        mock_request.side_effect = [mock_token, get_response(200, expected_response)]

        with Client(client_config) as client:
            result = client.get(full_url, params, merged_headers)

        assert result == expected_response
        assert mock_request.call_count == 2
        mock_request.assert_called_with(
            "GET",
            full_url,
            headers=expected_headers,
            params=params,
            timeout=300
        )

    @pytest.mark.parametrize(
        "error",
        [ConnectionError, Timeout, ChunkedEncodingError],
        ids=["ConnectionError", "Timeout", "ChunkedEncodingError"]
    )
    @pytest.mark.parametrize(
        "endpoint",
        ["/me/messages", "/me/events"],
        ids=["messages", "events"]
    )
    @patch("requests.Session.request")
    def test_request_errors_retry(self, mock_request, client_config, mock_token, error, endpoint):
        """
        Test that transient network errors trigger retries and eventually raise the
        corresponding exception after max retries.
        """
        full_url = f"{self.base_url}{endpoint}"
        mock_request.side_effect = [mock_token] + [error()] * 5

        with pytest.raises(error):
            with Client(client_config) as client:
                client.get(full_url, {}, self.default_headers)

        assert mock_request.call_count == 6  # 1 for token + 5 retries

    @pytest.mark.parametrize(
        "retry_after",
        ["1", "5", "10"],
        ids=["retry_after_1", "retry_after_5", "retry_after_10"]
    )
    @pytest.mark.parametrize(
        "endpoint",
        ["/me/messages", "/me/contacts"],
        ids=["messages", "contacts"]
    )
    @patch("requests.Session.request")
    def test_rate_limit_error(self, mock_request, client_config, mock_token, endpoint, retry_after):
        """
        Test that hitting the rate limit (429) with Retry-After headers causes the
        client to raise MsGraphRateLimitError after retry attempts.
        """
        full_url = f"{self.base_url}{endpoint}"

        mock_request.side_effect = [mock_token] + [
            get_response(429, {}, headers={"Retry-After": retry_after}, raise_error=True)
        ] * 5

        with pytest.raises(MsGraphRateLimitError):
            with Client(client_config) as client:
                client.get(full_url, {}, self.default_headers)

        assert mock_request.call_count == 6

    @pytest.mark.parametrize(
        "endpoint",
        ["/me/messages", "/me/events", "/me/contacts"],
        ids=["messages", "events", "contacts"]
    )
    @patch("requests.Session.request")
    def test_authorization_error(self, mock_request, client_config, mock_token, endpoint):
        """
        Test that HTTP 401 Unauthorized responses raise the MsGraphUnauthorizedError
        and contain the expected error message.
        """
        full_url = f"{self.base_url}{endpoint}"
        mock_request.side_effect = [mock_token, get_response(401, {}, raise_error=True)]

        with pytest.raises(MsGraphUnauthorizedError) as excinfo:
            with Client(client_config) as client:
                client.get(full_url, {}, self.default_headers)

        assert mock_request.call_count == 2
        assert "HTTP-error-code: 401" in str(excinfo.value)

    @pytest.mark.parametrize(
        "retry_after",
        ["3", "6"],
        ids=["retry_after_3", "retry_after_6"]
    )
    @pytest.mark.parametrize(
        "endpoint",
        ["/me/messages", "/me/events"],
        ids=["messages", "events"]
    )
    @patch("time.sleep", return_value=None)
    @patch("requests.Session.request")
    def test_rate_limit_with_backoff_sleep(self, mock_request, mock_sleep, client_config, mock_token, endpoint, retry_after):
        """
        Test that the client respects Retry-After header delays by calling time.sleep()
        during rate limit backoff retries.
        """
        full_url = f"{self.base_url}{endpoint}"

        mock_request.side_effect = [mock_token] + [
            get_response(
                429,
                json_data={"error": "Rate limit exceeded"},
                headers={"Retry-After": retry_after},
                raise_error=True
            )
        ] * 5

        with pytest.raises(MsGraphRateLimitError):
            with Client(client_config) as client:
                client.get(full_url, {}, self.default_headers)

        assert mock_request.call_count == 6
        assert mock_sleep.call_count >= 1


@patch("tap_ms_graph.client.datetime")
@patch("requests.Session.request")
def test_access_token_expiry(mock_request, mock_datetime, client_config, mock_token):
    """
    Test that the client correctly sets access token and expiry time
    upon fetching a new token.
    """
    mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
    mock_request.side_effect = [mock_token]

    client = Client(client_config)
    client._get_access_token()
    assert client._access_token == "mocked_token"


@freeze_time("2025-01-01 12:00:00")
@patch("requests.Session.request")
def test_token_reuse_if_not_expired(mock_request, client_config, mock_token):
    """
    Test that the client reuses the access token without refetching
    it if the token has not expired yet.
    """
    mock_get_response = MagicMock()
    mock_get_response.status_code = 200
    mock_get_response.json.return_value = {"data": "fake response"}

    mock_request.side
