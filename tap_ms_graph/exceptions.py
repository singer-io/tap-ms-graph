class MsGraphError(Exception):
    """class representing Generic Http error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class MsGraphBackoffError(MsGraphError):
    """class representing backoff error handling."""
    pass

class MsGraphBadRequestError(MsGraphError):
    """class representing 400 status code."""
    pass

class MsGraphUnauthorizedError(MsGraphError):
    """class representing 401 status code."""
    pass


class MsGraphForbiddenError(MsGraphError):
    """class representing 403 status code."""
    pass

class MsGraphNotFoundError(MsGraphError):
    """class representing 404 status code."""
    pass

class MsGraphConflictError(MsGraphError):
    """class representing 406 status code."""
    pass

class MsGraphUnprocessableEntityError(MsGraphBackoffError):
    """class representing 409 status code."""
    pass

class MsGraphRateLimitError(MsGraphBackoffError):
    """class representing 429 status code."""
    def __init__(self, message=None, response=None):
        """Initialize the Amazon_AdsRateLimitError. Parses the 'Retry-After' header from the response (if present) and sets the
            `retry_after` attribute accordingly.
        """
        self.response = response

        # Retry-After header parsing
        retry_after = None
        if response and hasattr(response, 'headers'):
            raw_retry = response.headers.get('Retry-After')
            if raw_retry:
                try:
                    retry_after = int(raw_retry)
                except ValueError:
                    retry_after = None

        self.retry_after = retry_after
        base_msg = message or "Rate limit hit"
        retry_info = f"(Retry after {self.retry_after} seconds.)" \
            if self.retry_after is not None else "(Retry after unknown delay.)"
        full_message = f"{base_msg} {retry_info}"
        super().__init__(full_message, response=response)

class MsGraphInternalServerError(MsGraphBackoffError):
    """class representing 500 status code."""
    pass

class MsGraphNotImplementedError(MsGraphBackoffError):
    """class representing 501 status code."""
    pass

class MsGraphBadGatewayError(MsGraphBackoffError):
    """class representing 502 status code."""
    pass

class MsGraphServiceUnavailableError(MsGraphBackoffError):
    """class representing 503 status code."""
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": MsGraphBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": MsGraphUnauthorizedError,
        "message": "The access token provided is expired, revoked, malformed or invalid for other reasons."
    },
    403: {
        "raise_exception": MsGraphForbiddenError,
        "message": "You are missing the following required scopes: read"
    },
    404: {
        "raise_exception": MsGraphNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": MsGraphConflictError,
        "message": "The API request cannot be completed because the requested operation would conflict with an existing item."
    },
    422: {
        "raise_exception": MsGraphUnprocessableEntityError,
        "message": "The request content itself is not processable by the server."
    },
    429: {
        "raise_exception": MsGraphRateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    500: {
        "raise_exception": MsGraphInternalServerError,
        "message": "The server encountered an unexpected condition which prevented" \
            " it from fulfilling the request."
    },
    501: {
        "raise_exception": MsGraphNotImplementedError,
        "message": "The server does not support the functionality required to fulfill the request."
    },
    502: {
        "raise_exception": MsGraphBadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": MsGraphServiceUnavailableError,
        "message": "API service is currently unavailable."
    }
}
