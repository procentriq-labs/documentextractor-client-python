import mimetypes
import requests
import json
from typing import List, Optional, Union, Any
from uuid import UUID

from documentextractor_commons.models.transfer import (
    FileResponse,
    WorkflowCreate,
    WorkflowUpdate,
    WorkflowResponse,
    RunCreate,
    RunResponse,
    RunResult,
    RunResultResponseFormat,
)

from .exceptions import (
    DocumentExtractorAPIError,
    AuthenticationError,
    ForbiddenError,
    ClientRequestError,
    APIServerError,
)

class DocumentExtractorAPIClient:
    """
    Python client for the DocumentExtractor API.
    """

    def __init__(self, root_url: str, api_key: str):
        """
        Initializes the API client.

        :param root_url: The base URL for the API (e.g., "https://api.documentextractor.ai").
        :param api_key: Your API key for authentication.
        """
        self.root_url = root_url.rstrip('/')
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json" # Default, can be overridden
        }

    def _request(self, method: str, path: str, parse_json: bool = True, **kwargs) -> Union[Any, requests.Response]:
        url = f"{self.root_url}{path}"
        request_headers = self.headers.copy()
        if 'headers' in kwargs:
            request_headers.update(kwargs.pop('headers'))

        try:
            response = requests.request(method, url, headers=request_headers, **kwargs)
            response.raise_for_status()

            if not parse_json:
                return response

            if response.status_code == 204: # No Content
                return None
            return response.json()

        except requests.exceptions.HTTPError as e:
            error_details = None
            status_code = e.response.status_code if e.response is not None else None
            
            try:
                if e.response is not None:
                    error_details = e.response.json()
            except json.JSONDecodeError:
                if e.response is not None:
                    error_details = e.response.text
            
            if status_code == 401:
                raise AuthenticationError(details=error_details) from e
            elif status_code == 403:
                raise ForbiddenError(details=error_details) from e
            elif status_code and 400 <= status_code < 500:
                # Catches all other 4xx errors
                msg = f"Client error {status_code}"
                if error_details and isinstance(error_details, dict) and error_details.get('detail'):
                    msg = f"Client error {status_code}: {error_details['detail']}"
                elif isinstance(error_details, str) and len(error_details) < 100: # Arbitrary length for short error strings
                    msg = f"Client error {status_code}: {error_details}"

                raise ClientRequestError(message=msg, status_code=status_code, details=error_details) from e
            elif status_code and 500 <= status_code < 600:
                msg = f"Server error {status_code}"
                raise APIServerError(message=msg, status_code=status_code, details=error_details) from e
            else:
                # Fallback for other HTTP errors or if status_code is None
                msg = f"HTTP Error {status_code if status_code else 'Unknown'} for {url}"
                raise DocumentExtractorAPIError(message=msg, status_code=status_code, details=error_details) from e
        
        except requests.exceptions.RequestException as e:
            # For network errors, timeouts, etc.
            raise DocumentExtractorAPIError(f"Request failed for {url}: {e}") from e

    # --- Files Endpoints ---
    def list_files(self) -> List[FileResponse]:
        response_data = self._request("GET", "/v1/files/")
        return [FileResponse(**item) for item in response_data]

    def upload_file(self, file_path: str, file_content: Optional[bytes] = None, filename: Optional[str] = None) -> FileResponse:
        """
        Upload a file and receive its ID and details.
        :param file_path: Path to the file to be uploaded (used for filename and mimetype if file_content is None).
        :param file_content: Optional bytes content of the file. If provided, filename must also be set.
        :param filename: Optional filename if file_content is provided.
        """
        if file_content is not None and filename is None:
            raise ValueError("filename must be provided if file_content is specified.")

        _filename = filename if filename else file_path.split('/')[-1]
        mime_type, _ = mimetypes.guess_type(file_path if file_content is None else _filename)
        if mime_type is None:
            mime_type = 'application/octet-stream'

        if file_content is not None:
            files = {'file': (_filename, file_content, mime_type)}
            response_data = self._request("POST", "/v1/files/", files=files)
        else:
            with open(file_path, 'rb') as f:
                files = {'file': (_filename, f, mime_type)}
                response_data = self._request("POST", "/v1/files/", files=files)
        return FileResponse(**response_data)

    def get_file(self, file_uuid: UUID) -> FileResponse:
        response_data = self._request("GET", f"/v1/files/{file_uuid}")
        return FileResponse(**response_data)

    def delete_file(self, file_uuid: UUID) -> None:
        self._request("DELETE", f"/v1/files/{file_uuid}", parse_json=False)
        return None

    # --- Workflows Endpoints ---
    def list_workflows(self) -> List[WorkflowResponse]:
        response_data = self._request("GET", "/v1/workflows/")
        return [WorkflowResponse(**item) for item in response_data]

    def create_workflow(self, workflow_data: WorkflowCreate) -> WorkflowResponse:
        response_data = self._request("POST", "/v1/workflows/", json=workflow_data.model_dump(exclude_none=True))
        return WorkflowResponse(**response_data)

    def get_workflow(self, workflow_uuid: UUID) -> WorkflowResponse:
        response_data = self._request("GET", f"/v1/workflows/{workflow_uuid}")
        return WorkflowResponse(**response_data)

    def override_workflow(self, workflow_uuid: UUID, workflow_data: WorkflowCreate) -> WorkflowResponse:
        response_data = self._request("PUT", f"/v1/workflows/{workflow_uuid}", json=workflow_data.model_dump(exclude_none=True))
        return WorkflowResponse(**response_data)

    def update_workflow(self, workflow_uuid: UUID, workflow_data: WorkflowUpdate) -> WorkflowResponse:
        response_data = self._request("PATCH", f"/v1/workflows/{workflow_uuid}", json=workflow_data.model_dump(exclude_none=True, exclude_unset=True))
        return WorkflowResponse(**response_data)

    def delete_workflow(self, workflow_uuid: UUID) -> None:
        self._request("DELETE", f"/v1/workflows/{workflow_uuid}", parse_json=False)
        return None # TODO return True or False; or at least raise error if unsuccessful (?already happens, right?)

    # --- Runs Endpoints ---
    def list_workflow_runs(self, workflow_uuid: UUID) -> List[RunResponse]:
        response_data = self._request("GET", f"/v1/workflows/{workflow_uuid}/runs/")
        return [RunResponse(**item) for item in response_data]

    def create_workflow_run(self, workflow_uuid: UUID, run_data: RunCreate) -> RunResponse:
        response_data = self._request("POST", f"/v1/workflows/{workflow_uuid}/runs/", json=run_data.model_dump())
        return RunResponse(**response_data)

    def get_workflow_run_details(self, workflow_uuid: UUID, run_num: int) -> RunResponse:
        response_data = self._request("GET", f"/v1/workflows/{workflow_uuid}/runs/{run_num}")
        return RunResponse(**response_data)

    def get_workflow_run_results(
        self,
        workflow_uuid: UUID,
        run_num: int,
        accept_format: RunResultResponseFormat = RunResultResponseFormat.JSON,
        format_option: Optional[str] = None
    ) -> Union[RunResult, str, bytes]:
        path = f"/v1/workflows/{workflow_uuid}/runs/{run_num}/results"
        params = {}
        if format_option:
            params['format_option'] = format_option

        custom_headers = self.headers.copy()
        custom_headers['Accept'] = accept_format.value # Enums from commons should have .value

        response = self._request("GET", path, params=params, headers=custom_headers, parse_json=False)

        if accept_format == RunResultResponseFormat.JSON:
            try:
                json_data = response.json()
                # TODO Assuming RunResult model from commons correctly matches the API response structure
                return RunResult(**json_data)
            except json.JSONDecodeError:
                raise ValueError("Failed to decode JSON response for RunResult.")
            except TypeError as e: # Catch Pydantic validation errors if structure doesn't match
                raise ValueError(f"Mismatched JSON structure for RunResult: {json_data}. Error: {e}")

        elif accept_format == RunResultResponseFormat.CSV:
            return response.text
        elif accept_format == RunResultResponseFormat.EXCEL:
            return response.content
        else:
            raise ValueError(f"Unsupported accept_format: {accept_format}")