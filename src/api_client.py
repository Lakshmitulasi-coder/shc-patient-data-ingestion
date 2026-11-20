"""API Integration and Fault Tolerance Client.

This module provides an enterprise-ready, fault-tolerant REST API client
for transmitting normalized patient records to the SHC platform.

Features:
- Bearer token authentication with credential masking in logs.
- Automatic payload batching (default max 100 records per request).
- Sliding-window rate limiter strictly enforcing <= 5 requests per second.
- Exponential backoff with jitter for HTTP 429, HTTP 5xx, and network errors.
- HTTP 429 'Retry-After' header support.
- Specialized HTTP 400 Bad Request isolation to 'failed_records.json' without
  halting the pipeline for subsequent batches.
- Comprehensive structured logging without secret leaks.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import random
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

logger = logging.getLogger("shc_ingestion.api_client")


def mask_token(token: Optional[str]) -> str:
    """Mask sensitive API tokens for safe logging.

    Args:
        token: Raw bearer token or secret string.

    Returns:
        Redacted string masking sensitive characters.
    """
    if not token:
        return "[EMPTY_TOKEN]"
    token_str = str(token).strip()
    if len(token_str) <= 6:
        return "[REDACTED]"
    return f"{token_str[:2]}****{token_str[-4:]}"


def chunk_records(records: List[Any], batch_size: int = 100) -> List[List[Any]]:
    """Divide a list of records into batches of up to batch_size.

    Args:
        records: List of patient records (dicts or objects).
        batch_size: Maximum elements per batch (default 100).

    Returns:
        List of batched sublists.
    """
    if batch_size <= 0:
        raise ValueError(f"batch_size must be a positive integer, got {batch_size}")
    return [records[i : i + batch_size] for i in range(0, len(records), batch_size)]


def build_headers(api_token: str) -> Dict[str, str]:
    """Construct standard HTTP headers with Bearer token authentication.

    Args:
        api_token: Bearer token secret.

    Returns:
        Dictionary of headers for REST API requests.
    """
    return {
        "Authorization": f"Bearer {api_token.strip()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "SHC-Patient-Ingestion-Engine/1.0",
    }


def calculate_backoff_delay(
    attempt: int,
    initial_backoff: float = 1.0,
    backoff_factor: float = 2.0,
    max_delay: float = 30.0,
    enable_jitter: bool = True,
) -> float:
    """Calculate exponential backoff delay with optional random jitter.

    Formula: delay = min(max_delay, initial_backoff * (backoff_factor ** (attempt - 1))) + jitter

    Args:
        attempt: 1-based attempt counter (1, 2, 3...).
        initial_backoff: Base delay in seconds.
        backoff_factor: Multiplier per attempt.
        max_delay: Cap on delay in seconds.
        enable_jitter: Whether to add uniform random jitter (0 to 0.5s).

    Returns:
        Delay time in seconds.
    """
    exp_delay = initial_backoff * (backoff_factor ** max(0, attempt - 1))
    capped_delay = min(exp_delay, max_delay)
    jitter = random.uniform(0.05, 0.5) if enable_jitter else 0.0
    return capped_delay + jitter


class RateLimiter:
    """Sliding-window rate limiter ensuring requests do not exceed max_rps."""

    def __init__(self, max_requests_per_second: float = 5.0) -> None:
        self.max_rps = max(0.1, float(max_requests_per_second))
        self.interval = 1.0  # 1-second rolling window
        self._timestamps: List[float] = []
        self._lock = threading.Lock()

    def acquire(self) -> float:
        """Block until a request can be executed within the rate limit.

        Returns:
            Sleep duration waited, in seconds.
        """
        with self._lock:
            now = time.monotonic()
            # Prune timestamps older than 1 second
            self._timestamps = [t for t in self._timestamps if now - t < self.interval]

            sleep_time = 0.0
            if len(self._timestamps) >= self.max_rps:
                oldest = self._timestamps[0]
                sleep_time = self.interval - (now - oldest) + 0.01
                if sleep_time > 0:
                    time.sleep(sleep_time)
                now = time.monotonic()
                self._timestamps = [t for t in self._timestamps if now - t < self.interval]

            self._timestamps.append(time.monotonic())
            return sleep_time


@dataclass
class BatchResult:
    """Outcome of an individual batch transmission."""

    batch_index: int
    status: str  # "SUCCESS", "FAILED_400", "FAILED_RETRIES_EXHAUSTED", "FAILED_FATAL"
    status_code: Optional[int]
    record_count: int
    retries_used: int
    error_message: Optional[str] = None
    records: Optional[List[Dict[str, Any]]] = None


@dataclass
class IngestionSummary:
    """High-level summary of the entire ingestion run."""

    total_records: int = 0
    total_batches: int = 0
    successful_batches: int = 0
    failed_batches: int = 0
    records_uploaded: int = 0
    records_failed: int = 0
    batch_results: List[BatchResult] = None

    def __post_init__(self) -> None:
        if self.batch_results is None:
            self.batch_results = []


def save_failed_records_to_file(
    failed_entries: List[Dict[str, Any]],
    file_path: str | Path,
) -> Path:
    """Persist failed batch records to JSON file with diagnostic metadata.

    Args:
        failed_entries: List of failed batch records and metadata.
        file_path: Path to target failed_records.json.

    Returns:
        Path of written JSON file.
    """
    path = Path(file_path)
    existing_data: List[Dict[str, Any]] = []

    if path.is_file():
        try:
            with open(path, mode="r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    existing_data = json.loads(content)
                    if not isinstance(existing_data, list):
                        existing_data = [existing_data]
        except (json.JSONDecodeError, OSError) as err:
            logger.warning("Failed to parse existing %s, will overwrite: %s", path, err)
            existing_data = []

    existing_data.extend(failed_entries)

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, mode="w", encoding="utf-8") as f:
        json.dump(existing_data, f, indent=2, ensure_ascii=False)

    logger.info("Saved %d failed record entries to %s", len(failed_entries), path.resolve())
    return path.resolve()


class SHCPatientAPIClient:
    """Client for transmitting patient records to SHC API with fault tolerance."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_token: Optional[str] = None,
        batch_size: int = 100,
        rate_limit_rps: float = 5.0,
        max_retries: int = 3,
        initial_backoff: float = 1.0,
        backoff_factor: float = 2.0,
        request_timeout: float = 30.0,
        failed_records_file: str | Path = "failed_records.json",
        session: Optional[requests.Session] = None,
    ) -> None:
        """Initialize API client with environment-aware defaults."""
        self.base_url = (base_url or os.getenv("API_BASE_URL", "https://api.mock-shc.com/v1/patients")).rstrip("/")
        self.api_token = api_token or os.getenv("API_TOKEN", "")
        self.batch_size = int(os.getenv("BATCH_SIZE", str(batch_size)))
        self.rate_limiter = RateLimiter(
            max_requests_per_second=float(os.getenv("RATE_LIMIT_RPS", str(rate_limit_rps)))
        )
        self.max_retries = int(os.getenv("MAX_RETRIES", str(max_retries)))
        self.initial_backoff = float(os.getenv("INITIAL_BACKOFF", str(initial_backoff)))
        self.backoff_factor = float(os.getenv("BACKOFF_FACTOR", str(backoff_factor)))
        self.request_timeout = float(os.getenv("REQUEST_TIMEOUT", str(request_timeout)))
        self.failed_records_file = Path(os.getenv("FAILED_RECORDS_FILE", str(failed_records_file)))
        self.session = session or requests.Session()

        masked_auth = mask_token(self.api_token)
        logger.info(
            "Initialized SHCPatientAPIClient: URL=%s, Token=%s, BatchSize=%d, MaxRetries=%d, RateLimit=%.1frps",
            self.base_url,
            masked_auth,
            self.batch_size,
            self.max_retries,
            self.rate_limiter.max_rps,
        )

    def send_batch(
        self,
        batch: List[Dict[str, Any]],
        batch_index: int = 1,
    ) -> BatchResult:
        """Send a single batch of patient records with rate-limiting and retries.

        Behavior:
          - HTTP 200/201: Success logged.
          - HTTP 400: Non-retriable bad request. Saved to failed_records.json.
                      Returns FAILED_400 without crashing the pipeline.
          - HTTP 429: Retried with exponential backoff or 'Retry-After' header.
          - HTTP 5xx: Retried with exponential backoff.
          - ConnectionError / Timeout: Retried with exponential backoff.
          - Exhausted retries: Saved to failed_records.json, returns failure.

        Args:
            batch: List of patient record dicts.
            batch_index: 1-based index of current batch for logging.

        Returns:
            BatchResult containing execution metrics.
        """
        record_count = len(batch)
        headers = build_headers(self.api_token)
        payload = {"patients": batch}

        logger.info(
            "Batch %d: Preparing to send %d records to %s",
            batch_index,
            record_count,
            self.base_url,
        )

        retries_used = 0

        for attempt in range(1, self.max_retries + 1):
            # Enforce rate limiter before each outbound request
            self.rate_limiter.acquire()

            try:
                start_time = time.monotonic()
                response = self.session.post(
                    self.base_url,
                    json=payload,
                    headers=headers,
                    timeout=self.request_timeout,
                )
                duration_ms = (time.monotonic() - start_time) * 1000

                status_code = response.status_code

                # Case 1: Success (2xx)
                if 200 <= status_code < 300:
                    logger.info(
                        "Batch %d: Successfully uploaded %d records (HTTP %d, latency %.1fms).",
                        batch_index,
                        record_count,
                        status_code,
                        duration_ms,
                    )
                    return BatchResult(
                        batch_index=batch_index,
                        status="SUCCESS",
                        status_code=status_code,
                        record_count=record_count,
                        retries_used=retries_used,
                    )

                # Case 2: HTTP 400 Bad Request (Non-retriable data payload error)
                if status_code == 400:
                    error_detail = response.text[:500]
                    logger.error(
                        "Batch %d: HTTP 400 Client Error received. Payload rejected by server: %s. "
                        "Isolating batch to %s; ingestion continues for subsequent batches.",
                        batch_index,
                        error_detail,
                        self.failed_records_file,
                    )

                    failure_entry = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "batch_index": batch_index,
                        "status_code": 400,
                        "error_type": "HTTP_400_BAD_REQUEST",
                        "error_message": error_detail,
                        "record_count": record_count,
                        "records": batch,
                    }
                    save_failed_records_to_file([failure_entry], self.failed_records_file)

                    return BatchResult(
                        batch_index=batch_index,
                        status="FAILED_400",
                        status_code=400,
                        record_count=record_count,
                        retries_used=retries_used,
                        error_message=error_detail,
                        records=batch,
                    )

                # Case 3: HTTP 429 Too Many Requests (Rate limit exceeded)
                if status_code == 429:
                    retries_used += 1
                    retry_after_header = response.headers.get("Retry-After")
                    if retry_after_header and retry_after_header.isdigit():
                        delay = float(retry_after_header)
                        logger.warning(
                            "Batch %d (attempt %d/%d): HTTP 429 Too Many Requests. "
                            "Server requested Retry-After=%s seconds.",
                            batch_index,
                            attempt,
                            self.max_retries,
                            retry_after_header,
                        )
                    else:
                        delay = calculate_backoff_delay(
                            attempt, self.initial_backoff, self.backoff_factor
                        )
                        logger.warning(
                            "Batch %d (attempt %d/%d): HTTP 429 Rate limited. "
                            "Backing off for %.2fs before retry.",
                            batch_index,
                            attempt,
                            self.max_retries,
                            delay,
                        )

                    if attempt < self.max_retries:
                        time.sleep(delay)
                        continue

                # Case 4: HTTP 5xx Server Errors (500, 502, 503, 504)
                elif 500 <= status_code < 600:
                    retries_used += 1
                    delay = calculate_backoff_delay(attempt, self.initial_backoff, self.backoff_factor)
                    logger.warning(
                        "Batch %d (attempt %d/%d): Server Error HTTP %d (%s). "
                        "Applying exponential backoff for %.2fs before retry.",
                        batch_index,
                        attempt,
                        self.max_retries,
                        status_code,
                        response.text[:200],
                        delay,
                    )
                    if attempt < self.max_retries:
                        time.sleep(delay)
                        continue

                # Case 5: Other client errors (e.g. 401 Unauthorized, 403 Forbidden, 404)
                else:
                    error_detail = response.text[:300]
                    logger.error(
                        "Batch %d: HTTP %d unexpected error. Response: %s",
                        batch_index,
                        status_code,
                        error_detail,
                    )
                    failure_entry = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "batch_index": batch_index,
                        "status_code": status_code,
                        "error_type": f"HTTP_{status_code}_ERROR",
                        "error_message": error_detail,
                        "record_count": record_count,
                        "records": batch,
                    }
                    save_failed_records_to_file([failure_entry], self.failed_records_file)
                    return BatchResult(
                        batch_index=batch_index,
                        status="FAILED_FATAL",
                        status_code=status_code,
                        record_count=record_count,
                        retries_used=retries_used,
                        error_message=error_detail,
                        records=batch,
                    )

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as net_err:
                retries_used += 1
                delay = calculate_backoff_delay(attempt, self.initial_backoff, self.backoff_factor)
                logger.warning(
                    "Batch %d (attempt %d/%d): Network exception (%s: %s). "
                    "Sleeping for %.2fs before retrying.",
                    batch_index,
                    attempt,
                    self.max_retries,
                    type(net_err).__name__,
                    str(net_err),
                    delay,
                )
                if attempt < self.max_retries:
                    time.sleep(delay)
                    continue

            except requests.exceptions.RequestException as req_err:
                retries_used += 1
                delay = calculate_backoff_delay(attempt, self.initial_backoff, self.backoff_factor)
                logger.error(
                    "Batch %d (attempt %d/%d): Request error: %s. Backing off %.2fs.",
                    batch_index,
                    attempt,
                    self.max_retries,
                    req_err,
                    delay,
                )
                if attempt < self.max_retries:
                    time.sleep(delay)
                    continue

        # Exhausted retries without 2xx
        logger.error(
            "Batch %d: Retries exhausted (%d attempts). Batch transmission failed. Saving to %s.",
            batch_index,
            self.max_retries,
            self.failed_records_file,
        )
        exhausted_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "batch_index": batch_index,
            "status_code": None,
            "error_type": "RETRIES_EXHAUSTED",
            "error_message": f"Exhausted {self.max_retries} attempts without receiving a 2xx response.",
            "record_count": record_count,
            "records": batch,
        }
        save_failed_records_to_file([exhausted_entry], self.failed_records_file)

        return BatchResult(
            batch_index=batch_index,
            status="FAILED_RETRIES_EXHAUSTED",
            status_code=None,
            record_count=record_count,
            retries_used=retries_used,
            error_message="Retries exhausted",
            records=batch,
        )

    def ingest_all(
        self,
        records: List[Any],
    ) -> IngestionSummary:
        """Partition records into batches and transmit them sequentially.

        Args:
            records: List of normalized records (dicts or PatientRecord objects).

        Returns:
            IngestionSummary with complete run statistics.
        """
        dict_records: List[Dict[str, Any]] = []
        for r in records:
            if hasattr(r, "to_dict"):
                dict_records.append(r.to_dict())
            elif isinstance(r, dict):
                dict_records.append(r)
            else:
                dict_records.append(vars(r))

        summary = IngestionSummary(total_records=len(dict_records))

        if not dict_records:
            logger.warning("No records supplied for ingestion.")
            return summary

        batches = chunk_records(dict_records, self.batch_size)
        summary.total_batches = len(batches)

        logger.info(
            "Beginning API ingestion: %d total records partitioned into %d batch(es) of size <= %d.",
            len(dict_records),
            len(batches),
            self.batch_size,
        )

        for idx, batch in enumerate(batches, start=1):
            result = self.send_batch(batch, batch_index=idx)
            summary.batch_results.append(result)

            if result.status == "SUCCESS":
                summary.successful_batches += 1
                summary.records_uploaded += result.record_count
            else:
                summary.failed_batches += 1
                summary.records_failed += result.record_count

        logger.info(
            "API ingestion completed: Total=%d, Batches=%d, SuccessfulBatches=%d, "
            "FailedBatches=%d, RecordsUploaded=%d, RecordsFailed=%d",
            summary.total_records,
            summary.total_batches,
            summary.successful_batches,
            summary.failed_batches,
            summary.records_uploaded,
            summary.records_failed,
        )

        return summary
