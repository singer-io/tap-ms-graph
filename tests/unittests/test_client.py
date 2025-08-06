import pytest
from unittest.mock import patch
from datetime import datetime
import requests
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError

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
    return MockResponse(status_code, json_data, raise_error, headers)


@pytest.mark.parametrize("status_code", [200, 201, 204])
def test_raise_for_error_success(status_code):
    response = get_response(status_code, {"message": "OK"})
    raise_for_error(response)  # Should not raise


@pytest.mark.parametrize(
    "response_data, expected_exception, expected_msg_part",
    [
        ({"code": "BadRequest", "details": "Invalid input"}, MsGraphBadRequestError, "HTTP-error-code: 400"),
        ({"message": "Unauthorized access"}, MsGraphUnauthorizedError, "Unauthorized access"),
        (None, MsGraphInternalServerError, "HTTP-error-code: 500"),
        ({"message": "Unknown error received from the API."}, MsGraphError, "Unknown error"),
    ],
)
def test_raise_for_error_exceptions(response_data, expected_exception, expected_msg_part):
    code_map = {
        MsGraphBadRequestError: 400,
        MsGraphUnauthorizedError: 401,
        MsGraphInternalServerError: 500,
        MsGraphError: 418,
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
    base_url = "https://graph.microsoft.com/v1.0"

    @pytest.fixture(autouse=True)
    def set_expected_headers(self):
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
        ]
    )
    @patch("requests.Session.request")
    def test_successful_request_multiple_cases(self, mock_request, client_config, mock_token,
                                               endpoint, params, headers, expected_response):
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
        "error", [ConnectionError, Timeout, ChunkedEncodingError]
    )
    @pytest.mark.parametrize(
        "endpoint", ["/me/messages", "/me/events"]
    )
    @patch("requests.Session.request")
    def test_request_errors_retry(self, mock_request, client_config, mock_token, error, endpoint):
        full_url = f"{self.base_url}{endpoint}"
        mock_request.side_effect = [mock_token] + [error()] * 5

        with pytest.raises(error):
            with Client(client_config) as client:
                client.get(full_url, {}, self.default_headers)

        assert mock_request.call_count == 6  # 1 for token + 5 retries

    @pytest.mark.parametrize("retry_after", ["1", "5", "10"])
    @pytest.mark.parametrize("endpoint", ["/me/messages", "/me/contacts"])
    @patch("requests.Session.request")
    def test_rate_limit_error(self, mock_request, client_config, mock_token, endpoint, retry_after):
        full_url = f"{self.base_url}{endpoint}"

        mock_request.side_effect = [mock_token] + [
            get_response(429, {}, headers={"Retry-After": retry_after}, raise_error=True)
        ] * 5

        with pytest.raises(MsGraphRateLimitError):
            with Client(client_config) as client:
                client.get(full_url, {}, self.default_headers)

        assert mock_request.call_count == 6

    @pytest.mark.parametrize("endpoint", ["/me/messages", "/me/events", "/me/contacts"])
    @patch("requests.Session.request")
    def test_authorization_error(self, mock_request, client_config, mock_token, endpoint):
        full_url = f"{self.base_url}{endpoint}"
        mock_request.side_effect = [mock_token, get_response(401, {}, raise_error=True)]

        with pytest.raises(MsGraphUnauthorizedError) as excinfo:
            with Client(client_config) as client:
                client.get(full_url, {}, self.default_headers)

        assert mock_request.call_count == 2
        assert "HTTP-error-code: 401" in str(excinfo.value)

    @pytest.mark.parametrize("retry_after", ["3", "6"])
    @pytest.mark.parametrize("endpoint", ["/me/messages", "/me/events"])
    @patch("time.sleep", return_value=None)
    @patch("requests.Session.request")
    def test_rate_limit_with_backoff_sleep(self, mock_request, mock_sleep, client_config, mock_token, endpoint, retry_after):
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
    mock_datetime.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
    mock_request.side_effect = [mock_token]

    client = Client(client_config)
    client._get_access_token()
    assert client._access_token == "mocked_token"
