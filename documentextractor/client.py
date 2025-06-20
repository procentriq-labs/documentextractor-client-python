import mimetypes
import requests
import json
import asyncio
import time
import os
from pydantic import ValidationError
from http import HTTPStatus
from typing import List, Optional, Union, Any, IO
from uuid import UUID

from documentextractor_commons.models.core import (
    FileType,
    RunStatus,
)
from documentextractor_commons.models.transfer import (
    CreateFileRequest,
    FileExtractionResult,
    FileResponse,
    WorkflowCreate,
    WorkflowUpdate,
    WorkflowResponse,
    RunCreate,
    RunResponse,
    RunResult,
    SchemaResponse,
    UploadRequest,
    UploadResponse,
)

from .exceptions import (
    DocumentExtractorAPIError,
    AuthenticationError,
    ForbiddenError,
    ClientRequestError,
    APIServerError,
    RunFailedError,
    RunTimeoutError
)

import logging

logger = logging.getLogger(__name__)

# --- Forward Declarations for Type Hinting ---
class DocumentExtractorAPIClient: pass
class File: pass
class Workflow: pass
class Run: pass
class RunResultsContainer: pass
class ExtractedDataItems: pass
class FilesCollection: pass
class WorkflowsCollection: pass
class WorkflowRunsCollection: pass


# --- Resource Objects ---
class File:
    """Represents a single file resource in the DocumentExtractor API."""
    def __init__(self, root_client: DocumentExtractorAPIClient, response_data: FileResponse):
        self._root_client = root_client
        self._response_data = response_data

    @property
    def data(self) -> FileResponse:
        """The underlying Pydantic response model for the file."""
        return self._response_data

    @property
    def id(self) -> UUID: return self.data.id
    @property
    def filename(self) -> str: return self.data.filename
    @property
    def filetype(self) -> FileType: return self.data.filetype
    @property
    def num_pages(self) -> int: return self.data.num_pages
    @property
    def size(self) -> int: return self.data.size

    def __repr__(self) -> str:
        return f"<File id='{self.id}' filename='{self.filename}'>"

    def refresh(self) -> "File":
        """Re-fetches the file's details from the API and updates the object."""
        fresh_data = self._root_client._request("GET", f"/v1/files/{self.id}")
        self._response_data = FileResponse(**fresh_data)
        return self

    def delete(self) -> None:
        """Deletes the file from the API."""
        self._root_client._request("DELETE", f"/v1/files/{self.id}", parse_json=False)


class Workflow:
    """Represents a single workflow resource."""
    def __init__(self, root_client: DocumentExtractorAPIClient, response_data: WorkflowResponse):
        self._root_client = root_client
        self._response_data = response_data
        self._runs_collection = None # For caching

    @property
    def data(self) -> WorkflowResponse:
        """The underlying Pydantic response model for the workflow."""
        return self._response_data
        
    @property
    def id(self) -> UUID: return self.data.id
    @property
    def name(self) -> str: return self.data.name
    @property
    def extraction_schema(self) -> SchemaResponse: return self.data.extraction_schema

    def __repr__(self) -> str:
        return f"<Workflow id='{self.id}' name='{self.name}'>"
        
    @property
    def runs(self) -> "WorkflowRunsCollection":
        """Provides access to the collection of runs for this workflow."""
        if self._runs_collection is None:
            self._runs_collection = WorkflowRunsCollection(self._root_client, self.id)
        return self._runs_collection

    def refresh(self) -> "Workflow":
        """Re-fetches the workflow's details from the API."""
        fresh_data = self._root_client._request("GET", f"/v1/workflows/{self.id}")
        self._response_data = WorkflowResponse(**fresh_data)
        return self

    def update(self, payload: WorkflowUpdate) -> "Workflow":
        """Partially updates the workflow's attributes (PATCH)."""
        updated_data = self._root_client._request(
            "PATCH", f"/v1/workflows/{self.id}", 
            data=payload.model_dump_json(exclude_unset=True)
        )
        self._response_data = WorkflowResponse(**updated_data)
        return self

    def override(self, payload: WorkflowCreate) -> "Workflow":
        """Completely replaces the workflow with new data (PUT)."""
        overridden_data = self._root_client._request(
            "PUT", f"/v1/workflows/{self.id}", 
            data=payload.model_dump_json()
        )
        self._response_data = WorkflowResponse(**overridden_data)
        return self

    def delete(self) -> None:
        """Deletes the workflow."""
        self._root_client._request("DELETE", f"/v1/workflows/{self.id}", parse_json=False)


class Run:
    """Represents a single workflow run."""
    def __init__(self, root_client: DocumentExtractorAPIClient, response_data: RunResponse):
        self._root_client = root_client
        self._response_data = response_data

    @property
    def data(self) -> RunResponse:
        """The underlying Pydantic response model for the run."""
        return self._response_data

    @property
    def run_num(self) -> int: return self.data.run_num
    @property
    def workflow_id(self) -> UUID: return self.data.workflow_id
    @property
    def status(self) -> RunStatus: return self.data.status
    
    def __repr__(self) -> str:
        return f"<Run num={self.run_num} workflow_id='{self.workflow_id}' status='{self.status.value}'>"

    def refresh(self) -> "Run":
        """Re-fetches the run's details from the API."""
        fresh_data = self._root_client._request(
            "GET", f"/v1/workflows/{self.workflow_id}/runs/{self.run_num}"
        )
        self._response_data = RunResponse(**fresh_data)
        return self

    def get_results(self) -> "RunResultsContainer":
        """Fetches the run results (in JSON format) and returns a container object."""
        # This will raise an error if the run is not in a completed state,
        # which is the desired behavior from the API.
        json_data = self._root_client._request(
            "GET", f"/v1/workflows/{self.workflow_id}/runs/{self.run_num}/results",
            headers={'Accept': 'application/json'}
        )
        result_model = RunResult(**json_data)
        return RunResultsContainer(self._root_client, self.workflow_id, self.run_num, result_model)


class ExtractedDataItems:
    """Provides access to the extracted items from a run result in various formats."""
    def __init__(self, root_client: DocumentExtractorAPIClient, workflow_id: UUID, run_num: int, items: List[FileExtractionResult]):
        self._root_client = root_client
        self._workflow_id = workflow_id
        self._run_num = run_num
        self._items = items
    
    @property
    def raw(self) -> List[FileExtractionResult]:
        """Returns the list of Pydantic FileExtractionResult models."""
        return self._items

    def as_csv(self) -> str:
        """Fetches the run results in CSV format from the API."""
        response = self._root_client._request(
            "GET", f"/v1/workflows/{self._workflow_id}/runs/{self._run_num}/results",
            headers={'Accept': 'text/csv'},
            parse_json=False
        )
        return response.text

    def as_excel(self) -> bytes:
        """Fetches the run results in Excel format from the API."""
        response = self._root_client._request(
            "GET", f"/v1/workflows/{self._workflow_id}/runs/{self._run_num}/results",
            headers={'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'},
            parse_json=False
        )
        return response.content


class RunResultsContainer:
    """A container for the results of a completed run."""
    def __init__(self, root_client: DocumentExtractorAPIClient, workflow_id: UUID, run_num: int, result_model: RunResult):
        self._root_client = root_client
        self._workflow_id = workflow_id
        self._run_num = run_num
        self._model = result_model
        self._extracted_data_obj = None # For caching

    @property
    def model(self) -> RunResult:
        """The underlying Pydantic RunResult model, containing both results and errors."""
        return self._model
    
    @property
    def errors(self) -> List[str]:
        """A list of any errors that occurred during the run."""
        return self.model.errors
    
    @property
    def extracted_data(self) -> ExtractedDataItems:
        """An object providing access to the extracted data items."""
        if self._extracted_data_obj is None:
            self._extracted_data_obj = ExtractedDataItems(
                self._root_client, self._workflow_id, self._run_num, self.model.results
            )
        return self._extracted_data_obj


# --- Collection Managers ---
class FilesCollection:
    """Manages operations for the collection of all files."""
    def __init__(self, root_client: DocumentExtractorAPIClient):
        self._root_client = root_client

    def list(self) -> List[File]:
        """Retrieve a list of all accessible files."""
        response_data = self._root_client._request("GET", "/v1/files/")
        return [File(self._root_client, FileResponse(**item)) for item in response_data]
    
    def get(self, file_uuid: UUID) -> File:
        """Retrieve a specific file by its UUID."""
        response_data = self._root_client._request("GET", f"/v1/files/{file_uuid}")
        return File(self._root_client, FileResponse(**response_data))

    def upload(
        self,
        file_path: Optional[str] = None,
        file_content: Optional[bytes] = None,
        file_stream: Optional[IO[bytes]] = None,
        filename: Optional[str] = None,
        mime_type: Optional[str] = None,
    ) -> File:
        """
        Uploads a file using a 3-step pre-signed URL process.

        This method is designed to be resilient and provide specific error feedback
        by leveraging the custom exception hierarchy.

        Args:
            file_path: The local path to the file to upload.
            file_content: The raw byte content of the file to upload.
            file_stream: An open file-like object (e.g., from open(..., 'rb')) to upload.
            filename: The name to assign to the file. If not provided, it will be inferred
                    from `file_path` or the `name` attribute of `file_stream`.
                    This parameter is required when using `file_content`.
            mime_type: MIME-Type of the file. Will be guessed if not supplied.

        Returns:
            A File object representing the uploaded file.
        
        Raises:
            ValueError: If the input arguments are invalid.
            AuthenticationError: If the API key is invalid (401).
            ForbiddenError: If the user lacks permissions for an action (403).
            ClientRequestError: For other 4xx errors from the API or S3.
            APIServerError: For 5xx errors from the API or S3.
            DocumentExtractorAPIError: For network issues or other unclassified errors.
        """
        num_inputs = sum(1 for item in [file_path, file_content, file_stream] if item is not None)
        if num_inputs != 1:
            raise ValueError("Exactly one of file_path, file_content, or file_stream must be provided.")

        _filename = None
        if filename:
            _filename = filename
        elif file_path:
            _filename = os.path.basename(file_path)
        elif file_stream and hasattr(file_stream, 'name') and file_stream.name:
            _filename = os.path.basename(file_stream.name)

        if not _filename:
            raise ValueError("A `filename` must be provided for `file_content` or for file streams that do not have a 'name' attribute.")

        if mime_type is None:
            mime_type, _ = mimetypes.guess_type(_filename)
            if mime_type is None:
                mime_type = 'application/octet-stream'

        announce_payload = UploadRequest(filename=_filename, content_type=mime_type)
        announce_response_dict = self._root_client._request(
            "POST",
            "/v1/uploads",
            data=announce_payload.model_dump_json()
        )
        
        try:
            announce_response = UploadResponse.model_validate(announce_response_dict)
        except ValidationError as e:
            raise APIServerError(
                message="API returned an invalid response during the announce step.",
                details=str(e)
            ) from e

        def perform_s3_upload(data_to_upload):
            try:
                response = requests.put(
                    announce_response.upload_url,
                    data=data_to_upload,
                    headers={'Content-Type': mime_type}
                )
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response is not None else None
                details = e.response.text if e.response is not None else None
                
                if status_code == 403:
                    raise ForbiddenError(
                        message="Permission denied by S3. The pre-signed URL may be expired or invalid.",
                        details=details
                    ) from e
                elif status_code and 400 <= status_code < 500:
                    raise ClientRequestError(
                        message=f"S3 returned a client error ({status_code}) during file upload.",
                        status_code=status_code,
                        details=details
                    ) from e
                elif status_code and 500 <= status_code < 600:
                    raise APIServerError(
                        message=f"S3 returned a server error ({status_code}) during file upload.",
                        status_code=status_code,
                        details=details
                    ) from e
                else:
                    raise DocumentExtractorAPIError(
                        message="An unexpected HTTP error occurred during S3 upload.",
                        status_code=status_code,
                        details=details
                    ) from e
            except requests.exceptions.RequestException as e:
                raise DocumentExtractorAPIError(f"A network error occurred during the S3 upload: {e}") from e

        if file_path:
            with open(file_path, 'rb') as f:
                perform_s3_upload(f)
        elif file_content:
            perform_s3_upload(file_content)
        elif file_stream:
            perform_s3_upload(file_stream)
            
        finalize_payload = CreateFileRequest(upload_token=announce_response.upload_token)
        response_data = self._root_client._request(
            "POST",
            "/v1/files",
            data=finalize_payload.model_dump_json()
        )
        
        return File(self._root_client, FileResponse(**response_data))


class WorkflowsCollection:
    """Manages operations for the collection of all workflows."""
    def __init__(self, root_client: DocumentExtractorAPIClient):
        self._root_client = root_client

    def list(self) -> List[Workflow]:
        """Retrieve a list of all workflows."""
        response_data = self._root_client._request("GET", "/v1/workflows/")
        return [Workflow(self._root_client, WorkflowResponse(**item)) for item in response_data]

    def get(self, workflow_uuid: UUID) -> Workflow:
        """Retrieve a specific workflow by its UUID."""
        response_data = self._root_client._request("GET", f"/v1/workflows/{workflow_uuid}")
        return Workflow(self._root_client, WorkflowResponse(**response_data))

    def create(self, payload: WorkflowCreate) -> Workflow:
        """Create a new extraction workflow."""
        # Use model_dump_json to correctly serialize UUIDs and other types
        response_data = self._root_client._request(
            "POST", "/v1/workflows/", 
            data=payload.model_dump_json()
        )
        return Workflow(self._root_client, WorkflowResponse(**response_data))


class WorkflowRunsCollection:
    """Manages operations for runs within a specific workflow."""
    def __init__(self, root_client: DocumentExtractorAPIClient, workflow_id: UUID):
        self._root_client = root_client
        self._workflow_id = workflow_id
    
    def list(self) -> List[Run]:
        """Retrieve a list of all runs for the parent workflow."""
        response_data = self._root_client._request("GET", f"/v1/workflows/{self._workflow_id}/runs/")
        return [Run(self._root_client, RunResponse(**item)) for item in response_data]

    def get(self, run_num: int) -> Run:
        """Retrieve a specific run by its number."""
        response_data = self._root_client._request("GET", f"/v1/workflows/{self._workflow_id}/runs/{run_num}")
        return Run(self._root_client, RunResponse(**response_data))

    def create(self, payload: RunCreate) -> Run:
        """Create and start a new run for the parent workflow."""
        response_data = self._root_client._request(
            "POST", f"/v1/workflows/{self._workflow_id}/runs/",
            data=payload.model_dump_json()
        )
        return Run(self._root_client, RunResponse(**response_data))
    
    async def create_and_wait_for_results(
        self,
        payload: RunCreate,
        polling_interval: int = 5,
        timeout: int = 300
    ) -> "RunResultsContainer":
        """
        Creates a new run, polls for its completion, and returns the results.

        This is a synchronous, blocking helper method that encapsulates the
        create -> poll -> get results workflow.

        :param payload: The RunCreate payload with file IDs.
        :param polling_interval: The time in seconds to wait between status checks.
        :param timeout: The maximum time in seconds to wait for the run to complete.
        :raises RunFailedError: If the run finishes with a FAILED or CANCELLED status.
        :raises RunTimeoutError: If the run does not complete within the specified timeout.
        :return: A RunResultsContainer object with the extraction results.
        """
        logger.debug(f"Starting run and polling for completion (interval: {polling_interval}s, timeout: {timeout}s)...")
        run = self.create(payload)
        logger.debug(f"  -> Run {run.run_num} created with status '{run.status.value}'.")

        start_time = time.time()
        while True:
            elapsed_time = time.time() - start_time
            if elapsed_time > timeout:
                raise RunTimeoutError(f"Run {run.run_num} did not complete within the {timeout}s timeout.")

            if run.status == RunStatus.COMPLETED:
                logger.debug(f"  -> Run {run.run_num} completed successfully.")
                return run.get_results()

            if run.status in [RunStatus.FAILED, RunStatus.CANCELLED]:
                logger.debug(f"  -> Run {run.run_num} finished with status '{run.status.value}'.")
                raise RunFailedError(
                    f"Run {run.run_num} finished with status '{run.status.value}'.",
                    run_status=run.status
                )

            logger.debug(f"  - Status is '{run.status.value}', waiting {polling_interval}s... (elapsed: {int(elapsed_time)}s)")
            await asyncio.sleep(polling_interval)
            run.refresh()


# --- Root API Client ---
class DocumentExtractorAPIClient:
    """Python client for the DocumentExtractor API."""

    def __init__(self, api_key: str, root_url: str = "https://api.documentextractor.ai"):
        """Initializes the API client."""
        if not root_url: raise ValueError("root_url cannot be empty.")
        self.root_url = root_url.rstrip('/')
        self.api_key = api_key
        self._headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }
        self.files = FilesCollection(self)
        self.workflows = WorkflowsCollection(self)

    def _request(self, method: str, path: str, parse_json: bool = True, **kwargs) -> Union[Any, requests.Response]:
        """Internal helper method to make requests to the API."""
        url = f"{self.root_url}{path}"
        
        request_headers = self._headers.copy()
        if 'headers' in kwargs:
            request_headers.update(kwargs.pop('headers'))
        
        # Conditionally set Content-Type for requests with a JSON body
        if 'data' in kwargs:
            request_headers['Content-Type'] = 'application/json'

        kwargs['headers'] = request_headers
        
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()

            if not parse_json: return response
            if response.status_code == HTTPStatus.NO_CONTENT: return None
            return response.json()
        except requests.exceptions.HTTPError as e:
            # Handle structured error raising
            status_code = e.response.status_code if e.response is not None else None
            details = None
            try:
                if e.response is not None: details = e.response.json().get('detail')
            except KeyError:
                if e.response is not None: details = e.response.json()
            except json.JSONDecodeError:
                if e.response is not None: details = e.response.text
            
            if status_code == HTTPStatus.UNAUTHORIZED: raise AuthenticationError(details=details) from e
            if status_code == HTTPStatus.FORBIDDEN: raise ForbiddenError(details=details) from e
            if status_code and HTTPStatus.BAD_REQUEST <= status_code < HTTPStatus.INTERNAL_SERVER_ERROR:
                raise ClientRequestError(message=f"Client Error: {status_code}", status_code=status_code, details=details) from e
            if status_code and HTTPStatus.INTERNAL_SERVER_ERROR <= status_code < 600:
                raise APIServerError(message=f"Server Error: {status_code}", status_code=status_code, details=details) from e
            raise DocumentExtractorAPIError(message=f"HTTP Error {status_code}", status_code=status_code, details=details) from e
        except requests.exceptions.RequestException as e:
            raise DocumentExtractorAPIError(f"Request failed for {url}: {e}") from e
