# DocumentExtractor.AI Python Client

A Python client for extracting structured data from documents using the [DocumentExtractor API](https://documentextractor.ai).  
It provides convenient methods to upload documents, define extraction schemas, start extraction workflows, and retrieve structured results.

---

## 📦 Installation

```bash
pip install documentextractor
```

---

## 🚀 Quick Example

You can get started with just a few lines of code:

```python
import os

from documentextractor import DocumentExtractorAPIClient, WorkflowCreate, SchemaCreate, RunCreate

api = DocumentExtractorAPIClient(
    api_key=os.environ.get("DOCUMENTEXTRACTOR_API_KEY"),
    root_url=os.environ.get("DOCUMENTEXTRACTOR_API_URL"),
)

# Upload a Document
file = api.files.upload("example_invoice.pdf")

# Define a workflow and extraction schema
workflow = api.workflows.create(payload=WorkflowCreate(
    name="Simple Invoice Extraction",
    extraction_schema=SchemaCreate(
    name="Invoice Schema",
    description="Extract invoice number and total amount",
    type="Text",
    is_array=False,
    children=[
        SchemaCreate(key="invoice_number", description="Sender-issued invoice number", type="Text", is_array=False),
        SchemaCreate(key="total_amount", description="Total amount payable", type="Number", is_array=False),
    ],
),
))

try:
    # Trigger a run
    results = await workflow.runs.create_and_wait_for_results(payload=RunCreate(file_ids=[file.id]))

    # Get structured results
    extracted_items = results.extracted_data.raw
    print(f"   - Found {len(extracted_items)} extracted item(s).")
    if extracted_items:
        # Pretty-print the structured data of the first result
        print("   - Data from first result:")
        import json
        print(json.dumps(extracted_items[0].data, indent=2))
except Exception as e:
    print(f"Error extracting data: {e}")
```

For the best experience, however, it's recommended to get more familiar with the client. For a full end-to-end usage script, see [`examples/basic_usage.py`](examples/basic_usage.py).

---

## 📄 License

Copyright © 2025 Philipp Heller

Licensed under the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).
You may not use this file except in compliance with the License.

See the [LICENSE](LICENSE) file for details.