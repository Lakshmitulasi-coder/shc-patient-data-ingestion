# Client Communication: Technical Integration & Deployment Summary

**To:** IT Director, GlobalHealth Corp  
**From:** Forward Deployed Engineering Team, SHC Technologies Pvt. Ltd.  
**Date:** November 20, 2026  
**Subject:** Technical Handover: Legacy Patient Data Extraction, Normalization & API Integration Engine  

---

Dear Director,

We have completed the deployment package for the **Legacy System Integration & Data Ingestion Pipeline** designed for GlobalHealth Corp. This solution extracts, sanitizes, and normalizes legacy patient management exports and prepares them for automated transmission to the SHC Platform REST API.

Please note that `https://api.mock-shc.com/v1/patients` is a mock specification endpoint; all network resilience, batching, rate limiting, and retry policies described below have been verified using automated mock HTTP test harnesses rather than live production uploads.

Below is an overview of the technical implementation, operational deployment instructions, and data quality handling.

---

## 1. Quick Start: Docker Build and Execution

The pipeline is packaged as a lightweight, hardened, multi-stage Docker container configured to run securely under an unprivileged Linux user (`appuser`, UID 1001).

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

The extraction engine parses raw CSV exports and outputs standardized records into `data/cleaned_patients.csv` following these rules:

1. **Missing Contact Information (Email & Phone):**
   - In accordance with healthcare data interoperability requirements, missing email addresses and phone numbers are normalized to empty strings (`""`) without crashing the pipeline.
2. **Date of Birth (DOB) Normalization to ISO 8601 (`YYYY-MM-DD`):**
   - Inconsistent date formats across facility records are parsed and standardized:
     - `MM/DD/YYYY` (e.g., `12/31/1992` &rarr; `1992-12-31`)
     - `DD-MM-YYYY` (e.g., `25-04-1975` &rarr; `1975-04-25`)
     - `YYYY-MM-DD` (e.g., `1980-05-15` &rarr; `1980-05-15`)
   - Invalid calendar dates (such as `1985-02-31`) are rejected and quarantined.
3. **Account Balance Standardization:**
   - Multi-currency symbols (`$`, `€`, `₹`, `£`) are stripped.
   - Comma thousand-separators (e.g., `"2,450.75"` &rarr; `2450.75`) are parsed into standard IEEE floating-point numbers.
   - Negative balances (e.g., `$-15.00` &rarr; `-15.00` or accounting parentheses `($120.50)` &rarr; `-120.50`) are preserved.
4. **Mandatory Field Validation:**
   - Every patient record must contain a non-empty `patient_id` and a valid `last_modified` timestamp. Records missing either field are quarantined and logged.
5. **Deduplication Logic:**
   - When duplicate `patient_id` values appear across the export, records are grouped together. The engine compares their `last_modified` timestamps and deterministically retains only the latest update, safely discarding stale entries.

---

## 3. Malformed CSV Row Recovery (`1,250.00`)

In the sample dataset, patient row `P-1002` (Jane Smith) contained an account balance `1,250.00` exported **without enclosing double quotes**.
- **The Problem:** Because standard comma-separated values (CSV) use commas as column delimiters, an unquoted comma inside a numeric literal splits `1` and `250.00` into two separate fields. This creates an extra column (9 columns instead of the expected 8), which causes naive parsers to shift timestamp values into balance fields or crash.
- **Our Safe Recovery Logic:** Rather than immediately dropping the row or blindly guessing, our parser applies a safe recovery heuristic:
  1. It verifies that the row has exactly one additional column (`len(row) == 9`).
  2. It verifies that the final column is a valid, parseable `last_modified` timestamp.
  3. It reconstructs the split balance tokens (`1` and `250.00`) into `1,250.00` and validates that it parses to numeric `1250.00`.
  4. It validates all other fields (`patient_id`, `dob`, contact fields).
  - If all checks succeed, the record is safely recovered and normalized.
  - If any condition is ambiguous or validation fails, the record is quarantined to prevent corrupt clinical data from propagating.

---

## 4. API Resilience, Batching, and Fault Tolerance

The API integration layer (`src/api_client.py`) incorporates enterprise reliability patterns (validated using automated mock HTTP test harnesses):

1. **Chunked Batching:** Records are batched into chunks of up to 100 patients per request (`BATCH_SIZE=100`), reducing network overhead.
2. **Absolute Rate Limiting:** Outbound traffic is strictly governed by a sliding-window rate limiter enforcing a hard ceiling of **at most 5 requests per second**, even if higher rates are configured.
3. **Exponential Backoff & Retries:**
   - Transient server errors (**HTTP 5xx**), rate-limit events (**HTTP 429**), and network timeouts are automatically retried with exponential backoff and random jitter (`delay = initial_backoff * 2^(attempt-1) + jitter`).
   - `MAX_RETRIES` specifies the maximum number of retry attempts following an initial failed request (e.g., `MAX_RETRIES=3` performs 1 initial attempt + up to 3 retries, totaling up to 4 attempts).
   - If an HTTP 429 response includes a `Retry-After` header, the client dynamically respects that duration.
4. **Isolated HTTP 400 Bad Request Handling:**
   - Client-side validation errors (**HTTP 400**) represent non-retriable payload defects.
   - The affected batch is immediately isolated and persisted into `failed_records.json` with diagnostic metadata (timestamp, HTTP status, server error message, and original record payload).
   - Ingestion proceeds uninterrupted for subsequent batches.
5. **Credential Security:**
   - Authorization tokens are injected via the `API_TOKEN` environment variable and passed as standard `Bearer` tokens. All logging layers automatically mask secret values (e.g., `mo****-xyz`), ensuring credentials are never exposed in logs or monitoring systems.

---

## 5. Recommended Upstream Improvement

To prevent data corruption at the source and eliminate the need for downstream heuristic recovery, we recommend the following upstream enhancement:

> **Recommendation:** Update the legacy export script or database query to strictly adhere to **RFC 4180 CSV specifications**.
>
> 1. **Enclose Delimiter-Containing Fields in Quotes:** Ensure that all fields containing commas, currency characters, or quotes are wrapped in double quotes (e.g., `"1,250.00"` instead of `1,250.00`).
> 2. **Standardize Date Formats at Extraction:** Configure the export routine to emit all dates of birth in ISO 8601 format (`YYYY-MM-DD`) and timestamps in UTC ISO 8601 (`YYYY-MM-DDTHH:MM:SSZ`).

Implementing RFC 4180 compliance upstream will ensure zero column-alignment anomalies and maximize automated ingestion throughput.

---

## 6. Support and Handover

The complete codebase, Docker configuration, and automated test suite have been placed in the repository. Please let us know if your team needs any assistance during deployment.

Warm regards,

**Forward Deployed Engineering Team**  
SHC Technologies Pvt. Ltd.
