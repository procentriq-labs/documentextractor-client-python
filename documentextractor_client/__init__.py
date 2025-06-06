from .client import DocumentExtractorAPIClient
from .exceptions import (
    DocumentExtractorAPIError,
    AuthenticationError,
    ForbiddenError,
    ClientRequestError,
    APIServerError
)

__version__ = "0.1.0"

__all__ = [
    "DocumentExtractorAPIClient",
    "DocumentExtractorAPIError",
    "AuthenticationError",
    "ForbiddenError",
    "ClientRequestError",
    "APIServerError",
]