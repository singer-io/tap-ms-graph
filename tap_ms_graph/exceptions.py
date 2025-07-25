class MS_GraphError(Exception):
    """class representing Generic Http error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class MS_GraphBackoffError(MS_GraphError):
    """class representing backoff error handling."""
    pass

class MS_GraphBadRequestError(MS_GraphError):
    """class representing 400 status code."""
    pass

class MS_GraphUnauthorizedError(MS_GraphError):
    """class representing 401 status code."""
    pass


class MS_GraphForbiddenError(MS_GraphError):
    """class representing 403 status code."""
    pass

class MS_GraphNotFoundError(MS_GraphError):
    """class representing 404 status code."""
    pass

class MS_GraphConflictError(MS_GraphError):
    """class representing 406 status code."""
    pass

class MS_GraphUnprocessableEntityError(MS_GraphBackoffError):
    """class representing 409 status code."""
    pass

class MS_GraphRateLimitError(MS_GraphBackoffError):
    """class representing 429 status code."""
    pass

class MS_GraphInternalServerError(MS_GraphBackoffError):
    """class representing 500 status code."""
    pass

class MS_GraphNotImplementedError(MS_GraphBackoffError):
    """class representing 501 status code."""
    pass

class MS_GraphBadGatewayError(MS_GraphBackoffError):
    """class representing 502 status code."""
    pass

class MS_GraphServiceUnavailableError(MS_GraphBackoffError):
    """class representing 503 status code."""
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": MS_GraphBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": MS_GraphUnauthorizedError,
        "message": "The access token provided is expired, revoked, malformed or invalid for other reasons."
    },
    403: {
        "raise_exception": MS_GraphForbiddenError,
        "message": "You are missing the following required scopes: read"
    },
    404: {
        "raise_exception": MS_GraphNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": MS_GraphConflictError,
        "message": "The API request cannot be completed because the requested operation would conflict with an existing item."
    },
    422: {
        "raise_exception": MS_GraphUnprocessableEntityError,
        "message": "The request content itself is not processable by the server."
    },
    429: {
        "raise_exception": MS_GraphRateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    500: {
        "raise_exception": MS_GraphInternalServerError,
        "message": "The server encountered an unexpected condition which prevented" \
            " it from fulfilling the request."
    },
    501: {
        "raise_exception": MS_GraphNotImplementedError,
        "message": "The server does not support the functionality required to fulfill the request."
    },
    502: {
        "raise_exception": MS_GraphBadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": MS_GraphServiceUnavailableError,
        "message": "API service is currently unavailable."
    }
}
