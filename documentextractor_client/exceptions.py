class DocumentExtractorAPIError(Exception):
    """Base exception for errors from the DocumentExtractoror API client."""
    def __init__(self, message, status_code=None, details=None):
        super().__init__(message)
        self.status_code = status_code
        self.details = details

    def __str__(self):
        base_message = super().__str__()
        details_str = f" - Details: {self.details}" if self.details else ""
        if self.status_code:
            return f"[Status Code: {self.status_code}] {base_message}{details_str}"
        return f"{base_message}{details_str}"

class AuthenticationError(DocumentExtractorAPIError):
    """Raised for authentication failures (HTTP 401 Unauthorized).
    This typically means the API key is missing, invalid, or expired."""
    def __init__(self, message="Authentication failed (401 Unauthorized)", details=None):
        super().__init__(message, status_code=401, details=details)

class ForbiddenError(DocumentExtractorAPIError):
    """Raised when authentication was successful but the authenticated user
    does not have permission to access the resource (HTTP 403 Forbidden)."""
    def __init__(self, message="Permission denied (403 Forbidden)", details=None):
        super().__init__(message, status_code=403, details=details)

class ClientRequestError(DocumentExtractorAPIError):
    """Raised for client-side errors other than 401 or 403 (e.g., 400, 404, 422)."""
    def __init__(self, message="Client request error", status_code=None, details=None):
        # status_code will be set by the _request method
        super().__init__(message, status_code=status_code, details=details)

class APIServerError(DocumentExtractorAPIError):
    """Raised for server-side errors (HTTP 5xx)."""
    def __init__(self, message="API server error", status_code=None, details=None):
        # status_code will be set by the _request method
        super().__init__(message, status_code=status_code, details=details)