"""
This script provides a complete, end-to-end example of how to use the
DocumentExtractor API client to set up and conduct an invoice data extraction.

The script performs the following steps:
1.  Uploads a sample PDF file ('example_invoice.pdf').
2.  Creates a new, detailed workflow specifically for invoice extraction.
3.  Triggers a run for the uploaded file using the created workflow.
4.  Polls the run's status until it completes.
5.  Fetches and prints the structured results if the run was successful.
6.  Cleans up by deleting the created workflow and file from the server.
"""
import os
import time
import requests

# --- Import the client and its exceptions ---
from documentextractor_client.client import DocumentExtractorAPIClient
from documentextractor_client.exceptions import (
    AuthenticationError,
    ClientRequestError,
    APIServerError,
)

# --- Import required Pydantic models from the commons library ---
from documentextractor_commons.models.transfer import (
    WorkflowCreate,
    SchemaCreate,
    RunCreate,
)
from documentextractor_commons.models.core import RunStatus


def create_invoice_schema_payload() -> WorkflowCreate:
    """
    Constructs the Pydantic object payload for creating the invoice workflow.
    This demonstrates how to build the nested schema structure programmatically.
    """
    # Define the nested 'sender' and 'recipient' address schemas
    address_children = [
        SchemaCreate(key="name", name="Name", type="Text", is_array=False, description="Name of invoice issuer / sender (e.g. company name)"),
        SchemaCreate(key="street", name="Street", type="Text", is_array=False, description="Address street of sender / issuer"),
        SchemaCreate(key="number", name="House Number", type="Text", is_array=False, description="House number of sender"),
        SchemaCreate(key="city", name="City", type="Text", is_array=False, description="City of sender address"),
        SchemaCreate(key="zip", name="Zip", type="Text", is_array=False, description="Zip or Postcode of sender address"),
        SchemaCreate(key="state", name="State", type="Text", is_array=False, description="State / Region of sender address (full name if possible - otherwise abbreviation)"),
        SchemaCreate(key="country_code", name="Country", type="Text", is_array=False, description="Two-letter country code per ISO for sender address"),
    ]

    # Define the nested 'line_items' schema
    line_items_children = [
        SchemaCreate(key="item_id", name="Item ID (Pos)", type="Number", is_array=False, description="Item ID or Position Number/ Index (Integer)"),
        SchemaCreate(key="item_name", name="Name", type="Text", is_array=False, description="Name or description of line item"),
        SchemaCreate(key="item_quantity", name="Qty", type="Number", is_array=False, description="Quantity of line item"),
        SchemaCreate(key="item_price", name="Price", type="Number", is_array=False, description="Unit price of item in invoice currency"),
        SchemaCreate(key="item_amount", name="Amount", type="Number", is_array=False, description="Total price of item in invoice currency"),
    ]

    # Define the main schema structure using the nested components
    extraction_schema = SchemaCreate(
        name="Invoice Schema",
        description="Extract invoice details",
        type="Text", # Root object type
        is_array=False,
        children=[
            SchemaCreate(key="invoice_number", name="Invoice Number", type="Text", is_array=False, description="Invoice number"),
            SchemaCreate(key="invoice_date", name="Invoice Date", type="Date", is_array=False, description="Date the invoice was issued, formatted YYYY-MM-DD"),
            SchemaCreate(key="due_date", name="Due Date", type="Date", is_array=False, description="Date the invoice is due by / needs to be paid by, formatted YYYY-MM-DD"),
            SchemaCreate(key="sender", name="Sender", type="Text", is_array=False, description="Sender of the invoice", children=address_children),
            SchemaCreate(key="recipient", name="Recipient", type="Text", is_array=False, description="Recipient or payee of the invoice", children=address_children),
            SchemaCreate(key="currency", name="Currency Code", type="Text", is_array=False, description="Currency of the invoice, three-letter code (e.g. USD, EUR, CHF)"),
            SchemaCreate(key="line_items", name="Line Items", type="Text", is_array=True, description="All line items / goods and services on the invoice", children=line_items_children),
            SchemaCreate(key="subtotal_amount", name="Subtotal (before taxes)", type="Number", is_array=False, description="Total amount of invoice before taxes"),
            SchemaCreate(key="tax_amount", name="Taxes", type="Number", is_array=False, description="Tax amount in invoice currency (if available, or 0)"),
            SchemaCreate(key="total_amount", name="Total Amount", type="Number", is_array=False, description="Total amount of invoice after taxes"),
        ],
    )

    # Create the final workflow payload
    workflow_payload = WorkflowCreate(
        name="Invoice Data Extraction",
        extraction_schema=extraction_schema,
    )

    return workflow_payload


def main():
    """
    Main function to execute the example workflow.
    """
    # --- Configuration ---
    # It's best to get these from environment variables
    API_ROOT_URL = os.getenv("DOCUMENTEXTRACTOR_API_URL", "https://api.documentextractor.ai")
    API_KEY = os.getenv("DOCUMENTEXTRACTOR_API_KEY")
    
    # Path to the example file you will place in this directory
    EXAMPLE_FILE_PATH = os.path.join(os.path.dirname(__file__), "example_invoice.pdf")

    if not API_KEY:
        print("ERROR: Please set the DOCUMENTEXTRACTOR_API_KEY environment variable to run this example.")
        return
        
    if not os.path.exists(EXAMPLE_FILE_PATH):
        print(f"ERROR: Example file not found at '{EXAMPLE_FILE_PATH}'.")
        print("Please place an 'example_invoice.pdf' file in the 'examples/' directory.")
        return

    # --- Initialize Client ---
    print("--- Initializing DocumentExtractor Client ---")
    api = DocumentExtractorAPIClient(root_url=API_ROOT_URL, api_key=API_KEY)
    
    # --- Resource Cleanup ---
    # Keep track of created resources to delete them at the end
    cleanup_resources = []

    try:
        # 1. Upload the test file
        print(f"\n1. Uploading file: '{EXAMPLE_FILE_PATH}'...")
        uploaded_file = api.files.upload(file_path=EXAMPLE_FILE_PATH)
        cleanup_resources.append(uploaded_file) # Schedule for deletion
        print(f"   -> Success! File uploaded with ID: {uploaded_file.id}")

        # 2. Create the extraction workflow
        print("\n2. Creating 'Invoice Data Extraction' workflow...")
        invoice_workflow_payload = create_invoice_schema_payload()
        created_workflow = api.workflows.create(payload=invoice_workflow_payload)
        cleanup_resources.append(created_workflow) # Schedule for deletion
        print(f"   -> Success! Workflow created with ID: {created_workflow.id}")

        # 3. Create and start a new run
        print(f"\n3. Triggering a run for workflow '{created_workflow.name}'...")
        run_payload = RunCreate(file_ids=[uploaded_file.id])
        # Use the 'runs' manager on the specific workflow object
        new_run = created_workflow.runs.create(payload=run_payload)
        print(f"   -> Success! Run created: {new_run}")

        # 4. Poll for run completion
        print("\n4. Polling for run completion (checking every 5 seconds)...")
        while new_run.status not in [RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED]:
            print(f"   - Current status is '{new_run.status.value}'. Waiting...")
            time.sleep(5)
            new_run.refresh()  # Updates the object's status in place
        
        print(f"   -> Finished! Final run status: '{new_run.status.value}'")

        # 5. Get and process the results
        if new_run.status == RunStatus.COMPLETED:
            print("\n5. Fetching and processing results...")
            
            results_container = new_run.get_results()

            # Access any errors from the run
            if results_container.errors:
                print(f"   - NOTE: Run completed with errors: {results_container.errors}")
            
            # Access the raw Pydantic model data
            extracted_items = results_container.extracted_data.raw
            print(f"   - Found {len(extracted_items)} extracted item(s).")
            if extracted_items:
                # Pretty-print the structured data of the first result
                print("   - Data from first result:")
                import json
                print(json.dumps(extracted_items[0].data, indent=2))

            # Demonstrate fetching alternative formats
            print("   - Fetching results as CSV...")
            csv_content = results_container.extracted_data.as_csv()
            print(f"     -> Received {len(csv_content)} characters of CSV data.")

            print("   - Fetching results as Excel...")
            excel_bytes = results_container.extracted_data.as_excel()
            print(f"     -> Received {len(excel_bytes)} bytes of Excel data.")

        else:
            print("\nRun did not complete successfully. Skipping results processing.")

    # --- Error Handling ---
    except AuthenticationError as e:
        print(f"\n[FATAL ERROR] Authentication failed. Check your API Key. Details: {e.details}")
    except ClientRequestError as e:
        print(f"\n[FATAL ERROR] A client error occurred (Status {e.status_code}). Details: {e.details}")
    except APIServerError as e:
        print(f"\n[FATAL ERROR] The API server returned an error (Status {e.status_code}). Details: {e.details}")
    except requests.exceptions.RequestException as e:
        print(f"\n[FATAL ERROR] A network error occurred. Is the API server at '{API_ROOT_URL}' running? Details: {e}")
    except Exception as e:
        print(f"\n[FATAL ERROR] An unexpected error occurred: {e}")

    # --- Cleanup ---
    finally:
        if cleanup_resources:
            print("\n--- Cleaning up created resources ---")
            # Reverse order to delete runs/workflows before files if needed by API
            for resource in reversed(cleanup_resources):
                try:
                    print(f"Deleting {resource.__class__.__name__} with ID: {resource.id}...")
                    resource.delete()
                    print("   -> Deleted successfully.")
                except Exception as e:
                    print(f"   -> ERROR: Failed to delete resource {resource.id}. Manual cleanup may be required. Reason: {e}")
        print("\nExample script finished.")


if __name__ == "__main__":
    main()
