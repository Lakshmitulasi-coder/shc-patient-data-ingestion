# Client Communication: Technical Integration & Deployment Summary

**To:** IT Director, GlobalHealth Corp  
**From:** Senior Backend & Forward Deployed Engineering Team, SHC Technologies Pvt. Ltd.  
**Date:** November 20, 2026  
**Subject:** Delivery & Deployment Guide: Legacy Patient Data Ingestion & API Integration Engine  

---

Dear Director,

We have completed the deployment-ready implementation of the **Legacy System Integration & Data Ingestion Pipeline** for GlobalHealth Corp. This solution bridges your legacy electronic health record (EHR) CSV exports with our modern REST API platform, ensuring high data integrity, strict fault tolerance, and zero data loss.

Below is an executive and technical summary of the delivery, including operational deployment instructions, data-cleaning methodology, API resilience architecture, and our upstream recommendations for your engineering team.

---

## 1. Quick Start: Docker Build and Execution

The pipeline is packaged as a lightweight, hardened, multi-stage Docker container designed to run securely under an unprivileged user (`appuser`, UID 1001).

### A. Building the Docker Image

From the root directory of the project, execute:

```bash
docker build -t shc-patient-ingestion:1.0.0 .
```

### B. Running the Docker Container

The container requires zero hardcoded secrets. All configuration is injected via environment variables or an environment file (`.env`), and volumes are mounted to seamlessly read input files and persist outputs.

#### Standard Run with Host Data Volume Mount:
```bash
docker run --rm \
  -e API_BASE_URL="https://api.mock-shc.com/v1/patients" \
  -e API_TOKEN="your-secure-bearer-token" \
  -e BATCH_SIZE=100 \
  -e MAX_RETRIES=3 \
  -v $(pwd)/data:/app/data \
  shc-patient-ingestion:1.0.0
```

#### Run Using an Environment File (`.env`):
```bash
docker run --rm \
  --env-file .env \
  -v $(pwd)/data:/app/data \
  shc-patient-ingestion:1.0.0
```

#### Dry-Run Mode (Validation & Cleaned CSV Export Only):
If you wish to test CSV extraction, validation, and deduplication without initiating outbound HTTP calls:
```bash
docker run --rm \
  -v $(pwd)/data:/app/data \
  shc-patient-ingestion:1.0.0 --dry-run
```

---

## 2. Data Cleaning & Normalization Overview

Legacy health data often arrives with formatting discrepancies, missing fields, and delimiter conflicts. Our extraction engine processes `data/patients.csv` and outputs clean, standardized data to `data/cleaned_patients.csv` following these rules:

1. **Missing Contact Information (Email & Phone):**
   - In accordance with healthcare data interoperability, missing email addresses and phone numbers are normalized to clean empty strings without triggering pipeline exceptions or crashes.
2. **Date of Birth (DOB) Normalization to ISO 8601 (`YYYY-MM-DD`):**
   - The engine automatically recognizes and standardizes three legacy formats:
     - `MM/DD/YYYY` (e.g., `04/15/1985` &rarr; `1985-04-15`)
     - `DD-MM-YYYY` (e.g., `23-08-1976` &rarr; `1976-08-23`)
     - `YYYY-MM-DD` (e.g., `1990-11-23` &rarr; `1990-11-23`)
   - Invalid calendar dates (such as `1985-02-31`) are strictly rejected and logged with diagnostic rationale.
3. **Account Balance Standardization:**
   - Multi-currency symbols (`$`, `€`, `₹`, `£`) are stripped.
   - Comma thousand-separators (e.g., `"1,250.00"` &rarr; `1250.00`) are parsed accurately into standard IEEE floating-point numbers.
   - Negative balances represented either via negative signs (`-$50.00`, `-50.00`) or standard accounting parentheses (`($120.50)` &rarr; `-120.50`) are strictly preserved.
4. **Mandatory Field Validation:**
   - Every patient record must have a non-empty `patient_id` and a valid `last_modified` timestamp. Records missing either field are rejected and logged.
5. **Deduplication Logic:**
   - When duplicate `patient_id` values appear across the export, records are grouped together. The engine compares their `last_modified` timestamps and deterministically retains only the latest update, safely discarding stale entries.

---

## 3. Handling the Malformed CSV Export Row (`1,250.00`)

During our initial ingestion audit, we detected a critical formatting error in your legacy CSV export:
- In row 12 (patient `P009`), the balance value `1,250.00` was emitted **without double quotation marks**.
- **The Issue:** Because standard comma-separated values (CSV) use commas as column delimiters, an unquoted comma inside a numeric literal splits `1` and `250.00` into two separate fields. This shifts all subsequent columns to the right, causing downstream systems to assign timestamp values to balance fields or crash due to unexpected column counts (9 columns instead of 8).
- **Our Handling:** Rather than silently misaligning data into incorrect fields, our parser detects the column mismatch, logs a high-priority warning citing the unquoted currency issue, and rejects the malformed row to preserve downstream clinical database integrity.
- **Reference Fix:** We have provided a validated sample in `data/patients_corrected.csv` showing the properly escaped format (`"1,250.00"`).

---

## 4. API Resilience, Batching, and Fault Tolerance

Outbound communication with `POST /v1/patients` incorporates enterprise-grade reliability patterns:

1. **Chunked Batching:** Records are batched into chunks of up to 100 patients per HTTP request (`BATCH_SIZE=100`), reducing network round trips and server overhead.
2. **Sliding-Window Rate Limiter:** An automated sliding-window rate limiter prevents API throttling by capping requests at a maximum of **5 requests per second**.
3. **Exponential Backoff & Retries:**
   - Transient server errors (**HTTP 5xx**), rate-limit events (**HTTP 429**), and network timeouts are automatically retried with exponential backoff and random jitter (`delay = initial_backoff * 2^(attempt-1) + jitter`).
   - If the API returns a `Retry-After` header during an HTTP 429 event, our client dynamically respects that server-specified window.
4. **Isolated HTTP 400 Bad Request Handling:**
   - Client-side validation errors (**HTTP 400**) represent payload-specific defects and are non-retriable.
   - Instead of aborting the ingestion job, the affected batch is immediately isolated and persisted into `failed_records.json` with comprehensive diagnostic metadata (timestamp, HTTP status code, API error message, and original record payload).
   - Ingestion then continues uninterrupted for all subsequent batches.
5. **Credential Security:**
   - Authorization tokens are injected via the `API_TOKEN` environment variable and passed as standard `Bearer` tokens. All logging layers automatically mask secret values (e.g., `mo****-xyz`), ensuring credentials are never exposed in logs or monitoring systems.

---

## 5. Recommended Upstream Improvement

To prevent data rejections before ingestion occurs, we recommend the following upstream enhancement to your database export routines:

> **Recommendation:** Update the legacy export stored procedure or script to strictly comply with **RFC 4180 CSV specifications**.
>
> 1. **Quote Delimiter-Containing Fields:** Ensure that all string or numeric fields containing commas, currency characters, or quotes are enclosed in standard double quotes (e.g., `"1,250.00"` instead of `1,250.00`).
> 2. **Native ISO 8601 Dates:** Configure the export service to format all dates as `YYYY-MM-DD` and timestamps as UTC ISO 8601 strings (`YYYY-MM-DDTHH:MM:SSZ`) at extraction time.

Adopting RFC 4180-compliant serialization upstream will eliminate column-shifting anomalies and maximize automated processing rates across all integration pipelines.

---

## 6. Support and Handover

All unit and integration test suites are passing with 100% test success across all edge cases. The complete codebase, Docker configuration, and documentation have been placed in the repository.

Please let us know if your infrastructure team requires any assistance during the staging rollout.

Warm regards,

**Senior Backend & DevOps Engineering Team**  
SHC Technologies Pvt. Ltd.  
`engineering@shc-technologies.com`
