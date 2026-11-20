"""API Integration and Fault Tolerance Client.

This module provides an enterprise-ready, fault-tolerant REST API client
for transmitting normalized patient records to the SHC platform.

Features:
- Bearer token authentication with credential masking in logs.
- Automatic payload batching strictly capped at a maximum of 100 records per request.
- Sliding-window rate limiter strictly enforcing an absolute maximum of <= 5
  requests per second, regardless of environment configuration.
- Exponential backoff with jitter for HTTP 429, HTTP 5xx, and network errors.
- Dynamic HTTP 429 'Retry-After' header support.
- Configurable MAX_RETRIES representing the number of retry attempts after initial failure.
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

logger = logging.getLogger("shc_ingestion.api_client")

# Absolute maximum API rate permitted by the SHC platform specification
MAX_ALLOWED_RPS: float = 5.0

# Absolute maximum records permitted in a single API batch payload
MAX_BATCH_SIZE: int = 100


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
    """Divide a list of records into batches clamped to a maximum of 100.

    Args:
        records: List of patient records (dicts or objects).
        batch_size: Requested elements per batch (clamped to max 100).

    Returns:
        List of batched sublists.

    Raises:
        ValueError: If batch_size is not a positive integer.
    """
    if batch_size <= 0:
        raise ValueError(f"batch_size must be a positive integer, got {batch_size}")

    # Enforce strict ceiling of MAX_BATCH_SIZE (100)
    effective_size = min(MAX_BATCH_SIZE, batch_size)
    return [records[i : i + effective_size] for i in range(0, len(records), effective_size)]


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
        attempt: 1-based retry attempt counter (1, 2, 3...).
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
    """Sliding-window rate limiter strictly enforcing max_requests_per_second."""

    def __init__(
        self,
        max_requests_per_second: float = 5.0,
        enforce_hard_cap: bool = True,
    ) -> None:
        """Initialize sliding-window rate limiter.

        Args:
            max_requests_per_second: Desired rate limit.
            enforce_hard_cap: If True, clamps rate to MAX_ALLOWED_RPS (5.0 req/s).
        """
        raw_rps = max(0.1, float(max_requests_per_second))
        if enforce_hard_cap:
            # Enforce an absolute maximum API rate of 5 requests per second per SHC platform specs
            self.max_rps = min(MAX_ALLOWED_RPS, raw_rps)
        else:
            self.max_rps = raw_rps

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
        """Initialize API client with environment-aware defaults.

        Args:
            base_url: REST endpoint URL.
            api_token: Bearer authentication token.
            batch_size: Maximum records per HTTP request (strictly clamped to max 100).
            rate_limit_rps: Desired requests per second (strictly clamped to max 5.0).
            max_retries: Maximum retry attempts following an initial failure (default 3).
            initial_backoff: Base exponential backoff delay in seconds.
            backoff_factor: Multiplier per retry attempt.
            request_timeout: Network socket timeout in seconds.
            failed_records_file: File path to persist failed 400/retried batches.
            session: Optional reusable requests.Session.
        """
        self.base_url = (base_url or os.getenv("API_BASE_URL", "https://api.mock-shc.com/v1/patients")).rstrip("/")
        self.api_token = api_token or os.getenv("API_TOKEN", "")

        # Enforce maximum batch size of 100 records regardless of configuration
        raw_batch_size = int(os.getenv("BATCH_SIZE", str(batch_size)))
        if raw_batch_size <= 0:
            raise ValueError(f"BATCH_SIZE must be a positive integer, got {raw_batch_size}")
        self.batch_size = min(MAX_BATCH_SIZE, raw_batch_size)

        # Enforce an absolute maximum API rate of 5 requests per second, even if configured above 5
        raw_configured_rps = float(os.getenv("RATE_LIMIT_RPS", str(rate_limit_rps)))
        effective_rps = min(MAX_ALLOWED_RPS, max(0.1, raw_configured_rps))
        self.rate_limiter = RateLimiter(
            max_requests_per_second=effective_rps,
            enforce_hard_cap=True,
        )

        # max_retries: number of retry attempts following an initial failure
        self.max_retries = max(0, int(os.getenv("MAX_RETRIES", str(max_retries))))
        self.initial_backoff = float(os.getenv("INITIAL_BACKOFF", str(initial_backoff)))
        self.backoff_factor = float(os.getenv("BACKOFF_FACTOR", str(backoff_factor)))
        self.request_timeout = float(os.getenv("REQUEST_TIMEOUT", str(request_timeout)))
        self.failed_records_file = Path(os.getenv("FAILED_RECORDS_FILE", str(failed_records_file)))
        self.session = session or requests.Session()

        masked_auth = mask_token(self.api_token)
        logger.info(
            "Initialized SHCPatientAPIClient: URL=%s, Token=%s, BatchSize=%d (Max=%d), MaxRetries=%d, RateLimit=%.1frps (HardCap=%.1f)",
            self.base_url,
            masked_auth,
            self.batch_size,
            MAX_BATCH_SIZE,
            self.max_retries,
            self.rate_limiter.max_rps,
            MAX_ALLOWED_RPS,
        )

    def send_batch(
        self,
        batch: List[Dict[str, Any]],
        batch_index: int = 1,
    ) -> BatchResult:
        """Send a single batch of patient records with rate-limiting and retries.

        Retry policy:
          - Initial request: Attempt 1
          - Subsequent retries: Up to max_retries retry attempts (attempts 2 through 1 + max_retries)
          - Total attempts possible: 1 + max_retries

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

        total_attempts = 1 + self.max_retries
        retries_used = 0

        for attempt_num in range(1, total_attempts + 1):
            is_retry = attempt_num > 1
            if is_retry:
                retries_used += 1

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
                        "Batch %d: Successfully uploaded %d records (HTTP %d, latency %.1fms, retries=%d).",
                        batch_index,
                        record_count,
                        status_code,
                        duration_ms,
                        retries_used,
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
                    retry_after_header = response.headers.get("Retry-After")
                    if retry_after_header and retry_after_header.isdigit():
                        delay = float(retry_after_header)
                        logger.warning(
                            "Batch %d (retry %d/%d): HTTP 429 Too Many Requests. "
                            "Server requested Retry-After=%s seconds.",
                            batch_index,
                            retries_used + 1,
                            self.max_retries,
                            retry_after_header,
                        )
                    else:
                        delay = calculate_backoff_delay(
                            retries_used + 1, self.initial_backoff, self.backoff_factor
                        )
                        logger.warning(
                            "Batch %d (retry %d/%d): HTTP 429 Rate limited. "
                            "Backing off for %.2fs before retry.",
                            batch_index,
                            retries_used + 1,
                            self.max_retries,
                            delay,
                        )

                    if attempt_num < total_attempts:
                        time.sleep(delay)
                        continue

                # Case 4: HTTP 5xx Server Errors (500, 502, 503, 504)
                elif 500 <= status_code < 600:
                    delay = calculate_backoff_delay(
                        retries_used + 1, self.initial_backoff, self.backoff_factor
                    )
                    logger.warning(
                        "Batch %d (retry %d/%d): Server Error HTTP %d (%s). "
                        "Applying exponential backoff for %.2fs before retry.",
                        batch_index,
                        retries_used + 1,
                        self.max_retries,
                        status_code,
                        response.text[:200],
                        delay,
                    )
                    if attempt_num < total_attempts:
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
                delay = calculate_backoff_delay(
                    retries_used + 1, self.initial_backoff, self.backoff_factor
                )
                logger.warning(
                    "Batch %d (retry %d/%d): Network exception (%s: %s). "
                    "Sleeping for %.2fs before retrying.",
                    batch_index,
                    retries_used + 1,
                    self.max_retries,
                    type(net_err).__name__,
                    str(net_err),
                    delay,
                )
                if attempt_num < total_attempts:
                    time.sleep(delay)
                    continue

            except requests.exceptions.RequestException as req_err:
                delay = calculate_backoff_delay(
                    retries_used + 1, self.initial_backoff, self.backoff_factor
                )
                logger.error(
                    "Batch %d (retry %d/%d): Request error: %s. Backing off %.2fs.",
                    batch_index,
                    retries_used + 1,
                    self.max_retries,
                    req_err,
                    delay,
                )
                if attempt_num < total_attempts:
                    time.sleep(delay)
                    continue

        # Exhausted all retries without 2xx
        logger.error(
            "Batch %d: Exhausted %d retry attempts (total %d requests made). Batch transmission failed. Saving to %s.",
            batch_index,
            self.max_retries,
            total_attempts,
            self.failed_records_file,
        )
        exhausted_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "batch_index": batch_index,
            "status_code": None,
            "error_type": "RETRIES_EXHAUSTED",
            "error_message": f"Exhausted {self.max_retries} retry attempts ({total_attempts} total requests) without receiving a 2xx response.",
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
