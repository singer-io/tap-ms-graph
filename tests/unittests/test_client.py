import pytest
from unittest.mock import patch
from datetime import datetime, timedelta
import requests
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError

from tap_ms_graph.client import Client, raise_for_error
from tap_ms_graph.exceptions import (
    ERROR_CODE_EXCEPTION_MAPPING,
    MS_GraphError,
    MS_GraphUnauthorizedError,
    MS_GraphBadRequestError,
    MS_GraphRateLimitError,
    MS_GraphInternalServerError,
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
    return MockResponse(status_code, json_data, raise_error, headers)


@pytest.mark.parametrize("status_code", [200, 201, 204])
def test_raise_for_error_success(status_code):
    response = get_response(status_code, {"message": "OK"})
    raise_for_error(response)  # Should not raise


@pytest.mark.parametrize(
    "response_data, expected_exception, expected_msg_part",
    [
        ({"code": "BadRequest", "details": "Invalid input"}, MS_GraphBadRequestError, "HTTP-error-code: 400"),
        ({"message": "Unauthorized access"}, MS_GraphUnauthorizedError, "Unauthorized access"),
        (None, MS_GraphInternalServerError, "HTTP-error-code: 500"),
        ({"message": "I'm new exception"}, MS_GraphError, "I'm new exception"),
    ],
)
def test_raise_for_error_exceptions(response_data, expected_exception, expected_msg_part):
    code_map = {
        MS_GraphBadRequestError: 400,
        MS_GraphUnauthorizedError: 401,
        MS_GraphInternalServerError: 500,
        MS_GraphError: 418,
    }
    status_code = code_map[expected_exception]
    if status_code == 418 and 418 in ERROR_CODE_EXCEPTION_MAPPING:
        del ERROR_CODE_EXCEPTION_MAPPING[418]

    response = get_response(status_code, response_data, raise_error=True)
    with pytest.raises(expected_exception) as excinfo:
        raise_for_error(response)
    assert expected_msg_part in str(excinfo.value)


@pytest.fixture
def client_config():
    return {
        "user_agent": "singer",
        "client_id": "mocked_client_id",
        "client_secret": "mocked_secret",
        "tenant_id": "mocked_tenant"
    }


@pytest.fixture
def mock_token():
    return get_response(
        200,
        {
            "access_token": "mocked_token",
            "expires_in": 3600
        }
    )


class TestClientRequests:
    url = "https://graph.microsoft.com/v1.0/me/messages"
    params = {}
    headers = {
        'User-Agent': 'singer',
        'Content-Type': 'application/json',
    }

    @pytest.fixture(autouse=True)
    def set_expected_headers(self):
        self.expected_headers = dict(self.headers)
        self.expected_headers["Authorization"] = "mocked_token"

    @patch("requests.Session.request")
    def test_successful_request(self, mock_request, client_config, mock_token):
        # First call: Token, Second call: Actual GET
        mock_request.side_effect = [mock_token, get_response(200, {"result": []})]

        with Client(client_config) as client:
            result = client.get(self.url, self.params, self.headers)

        assert result == {"result": []}
        assert mock_request.call_count == 2
        mock_request.assert_called_with(
            "GET",
            self.url,
            headers=self.expected_headers,
            params=self.params,
            timeout=300
        )

    @pytest.mark.parametrize("error", [ConnectionError, Timeout, ChunkedEncodingError])
    @patch("requests.Session.request")
    def test_request_errors_retry(self, mock_request, client_config, mock_token, error):
        mock_request.side_effect = [mock_token] + [error()] * 5
        with pytest.raises(error):
            with Client(client_config) as client:
                client.get(self.url, self.params, self.headers)
        assert mock_request.call_count == 6  # 1 for token + 5 retries

    @patch("requests.Session.request")
    def test_rate_limit_error(self, mock_request, client_config, mock_token):
        mock_request.side_effect = [mock_token] + [
            get_response(429, {}, headers={"Retry-After": "3"}, raise_error=True)
        ] * 5

        with pytest.raises(MS_GraphRateLimitError):
            with Client(client_config) as client:
                client.get(self.url, self.params, self.headers)
        assert mock_request.call_count == 6  # 1 token + 5 retries

    @patch("requests.Session.request")
    def test_authorization_error(self, mock_request, client_config, mock_token):
        mock_request.side_effect = [mock_token, get_response(401, {}, raise_error=True)]

        with pytest.raises(MS_GraphUnauthorizedError) as excinfo:
            with Client(client_config) as client:
                client.get(self.url, self.params, self.headers)

        assert mock_request.call_count == 2
        assert "HTTP-error-code: 401" in str(excinfo.value)

    @patch("time.sleep", return_value=None)
    @patch("requests.Session.request")
    def test_rate_limit_with_backoff_sleep(self, mock_request, mock_sleep, client_config, mock_token):
        # First call returns the token
        # Next 5 simulate 429 Too Many Requests with Retry-After: 3
        mock_request.side_effect = [mock_token] + [
            get_response(
                429,
                json_data={"error": "Rate limit exceeded"},
                headers={"Retry-After": "3"},
                raise_error=True
            )
        ] * 5

        with pytest.raises(MS_GraphRateLimitError):
            with Client(client_config) as client:
                client.get(self.url, self.params, self.headers)

        assert mock_request.call_count == 6  # 1 token + 5 retries
        assert mock_sleep.call_count >= 1  # Confirm backoff sleep was triggered


@patch("tap_ms_graph.client.datetime")
@patch("requests.Session.request")
def test_access_token_expiry(mock_request, mock_datetime, client_config, mock_token):
    mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
    mock_request.side_effect = [mock_token]

    client = Client(client_config)
    client._get_access_token()
    assert client._access_token == "mocked_token"
