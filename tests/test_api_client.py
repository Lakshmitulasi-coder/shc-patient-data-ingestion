"""Unit tests for API Integration and Fault Tolerance (Task 2)."""

import json
from pathlib import Path
import time
import pytest
import requests
import responses

from src.api_client import (
    MAX_ALLOWED_RPS,
    RateLimiter,
    SHCPatientAPIClient,
    build_headers,
    calculate_backoff_delay,
    chunk_records,
    mask_token,
    save_failed_records_to_file,
)


class TestUtilityFunctions:
    """Test suite for helper and utility functions."""

    def test_mask_token(self) -> None:
        assert mask_token(None) == "[EMPTY_TOKEN]"
        assert mask_token("") == "[EMPTY_TOKEN]"
        assert mask_token("12345") == "[REDACTED]"
        assert mask_token("mock-shc-token-secret-xyz") == "mo****-xyz"

    def test_chunk_records(self) -> None:
        items = list(range(250))
        chunks = chunk_records(items, batch_size=100)
        assert len(chunks) == 3
        assert len(chunks[0]) == 100
        assert len(chunks[1]) == 100
        assert len(chunks[2]) == 50

    def test_chunk_records_invalid_size(self) -> None:
        with pytest.raises(ValueError):
            chunk_records([1, 2, 3], batch_size=0)

    def test_build_headers(self) -> None:
        headers = build_headers("my-secret-token")
        assert headers["Authorization"] == "Bearer my-secret-token"
        assert headers["Content-Type"] == "application/json"
        assert "User-Agent" in headers

    def test_backoff_delay_calculation(self) -> None:
        # Without jitter for deterministic assertion
        delay1 = calculate_backoff_delay(1, initial_backoff=1.0, backoff_factor=2.0, enable_jitter=False)
        delay2 = calculate_backoff_delay(2, initial_backoff=1.0, backoff_factor=2.0, enable_jitter=False)
        delay3 = calculate_backoff_delay(3, initial_backoff=1.0, backoff_factor=2.0, enable_jitter=False)

        assert delay1 == 1.0
        assert delay2 == 2.0
        assert delay3 == 4.0

    def test_save_failed_records_persists_data(self, tmp_path: Path) -> None:
        failed_file = tmp_path / "failed_records.json"
        entry1 = {"batch_index": 1, "error": "HTTP 400", "records": [{"id": "P001"}]}
        entry2 = {"batch_index": 2, "error": "HTTP 500", "records": [{"id": "P002"}]}

        save_failed_records_to_file([entry1], failed_file)
        save_failed_records_to_file([entry2], failed_file)

        saved = json.loads(failed_file.read_text(encoding="utf-8"))
        assert len(saved) == 2
        assert saved[0]["batch_index"] == 1
        assert saved[1]["batch_index"] == 2


class TestRateLimiter:
    """Test suite for sliding-window rate limiter."""

    def test_rate_limiter_enforces_hard_cap_of_5(self) -> None:
        # Even when configured above 5.0 (e.g. 50.0), hard cap must limit it to 5.0
        limiter = RateLimiter(max_requests_per_second=50.0, enforce_hard_cap=True)
        assert limiter.max_rps == 5.0
        assert limiter.max_rps == MAX_ALLOWED_RPS

    def test_client_enforces_rate_limit_hard_cap(self, monkeypatch) -> None:
        monkeypatch.setenv("RATE_LIMIT_RPS", "20.0")
        client = SHCPatientAPIClient()
        assert client.rate_limiter.max_rps == 5.0

    def test_rate_limiter_allows_burst_within_capacity(self) -> None:
        limiter = RateLimiter(max_requests_per_second=5.0)
        start = time.monotonic()
        for _ in range(5):
            limiter.acquire()
        duration = time.monotonic() - start
        # First 5 calls in empty window should execute immediately without sleep
        assert duration < 0.2

    def test_rate_limiter_throttles_sixth_call(self) -> None:
        limiter = RateLimiter(max_requests_per_second=5.0)
        start = time.monotonic()
        for _ in range(6):
            limiter.acquire()
        duration = time.monotonic() - start
        # The 6th request must wait for the rolling window to advance
        assert duration >= 0.85


class TestSHCPatientAPIClient:
    """Test suite for API client with mocked responses."""

    @pytest.fixture
    def client(self, tmp_path: Path) -> SHCPatientAPIClient:
        return SHCPatientAPIClient(
            base_url="https://api.mock-shc.com/v1/patients",
            api_token="test-secret-token-12345",
            batch_size=100,
            rate_limit_rps=5.0,
            max_retries=3,  # 3 retry attempts following initial request
            initial_backoff=0.01,  # Short delays for fast unit tests
            backoff_factor=1.5,
            failed_records_file=tmp_path / "failed_records.json",
        )

    @responses.activate
    def test_successful_batch_upload(self, client: SHCPatientAPIClient) -> None:
        responses.add(
            responses.POST,
            "https://api.mock-shc.com/v1/patients",
            json={"status": "success", "processed": 2},
            status=200,
        )

        batch = [{"patient_id": "P001"}, {"patient_id": "P002"}]
        result = client.send_batch(batch, batch_index=1)

        assert result.status == "SUCCESS"
        assert result.status_code == 200
        assert result.record_count == 2
        assert result.retries_used == 0

        # Verify authentication header
        req = responses.calls[0].request
        assert req.headers["Authorization"] == "Bearer test-secret-token-12345"

    @responses.activate
    def test_http_400_bad_request_isolated_and_persisted(self, client: SHCPatientAPIClient) -> None:
        responses.add(
            responses.POST,
            "https://api.mock-shc.com/v1/patients",
            json={"error": "Invalid schema: Missing required field"},
            status=400,
        )

        batch = [{"patient_id": "P999", "invalid": True}]
        result = client.send_batch(batch, batch_index=1)

        # 400 is not retried; recorded as failure
        assert result.status == "FAILED_400"
        assert result.status_code == 400
        assert result.retries_used == 0

        # Verify saved to failed_records.json
        assert client.failed_records_file.exists()
        saved = json.loads(client.failed_records_file.read_text(encoding="utf-8"))
        assert len(saved) == 1
        assert saved[0]["status_code"] == 400
        assert saved[0]["error_type"] == "HTTP_400_BAD_REQUEST"

    @responses.activate
    def test_http_429_retried_and_succeeds(self, client: SHCPatientAPIClient) -> None:
        # First attempt returns 429, second attempt succeeds with 200
        responses.add(
            responses.POST,
            "https://api.mock-shc.com/v1/patients",
            headers={"Retry-After": "0"},
            status=429,
        )
        responses.add(
            responses.POST,
            "https://api.mock-shc.com/v1/patients",
            json={"status": "ok"},
            status=200,
        )

        batch = [{"patient_id": "P001"}]
        result = client.send_batch(batch, batch_index=1)

        assert result.status == "SUCCESS"
        assert result.status_code == 200
        assert result.retries_used == 1
        assert len(responses.calls) == 2

    @responses.activate
    def test_http_5xx_retried_and_succeeds(self, client: SHCPatientAPIClient) -> None:
        # First attempt returns 503 Service Unavailable, second returns 200
        responses.add(
            responses.POST,
            "https://api.mock-shc.com/v1/patients",
            json={"error": "Database overload"},
            status=503,
        )
        responses.add(
            responses.POST,
            "https://api.mock-shc.com/v1/patients",
            json={"status": "recovered"},
            status=200,
        )

        batch = [{"patient_id": "P001"}]
        result = client.send_batch(batch, batch_index=1)

        assert result.status == "SUCCESS"
        assert result.retries_used == 1
        assert len(responses.calls) == 2

    @responses.activate
    def test_retries_exhausted_performs_expected_retry_attempts(
        self, client: SHCPatientAPIClient
    ) -> None:
        # With max_retries=3: 1 initial attempt + 3 retries = 4 total requests
        for _ in range(4):
            responses.add(
                responses.POST,
                "https://api.mock-shc.com/v1/patients",
                json={"error": "Persistent internal error"},
                status=500,
            )

        batch = [{"patient_id": "P001"}]
        result = client.send_batch(batch, batch_index=1)

        assert result.status == "FAILED_RETRIES_EXHAUSTED"
        assert result.retries_used == 3
        assert len(responses.calls) == 4

        # Verify failed record written
        saved = json.loads(client.failed_records_file.read_text(encoding="utf-8"))
        assert len(saved) == 1
        assert saved[0]["error_type"] == "RETRIES_EXHAUSTED"

    @responses.activate
    def test_ingest_all_continues_after_http_400_batch(self, client: SHCPatientAPIClient) -> None:
        client.batch_size = 2

        # Batch 1 returns 400
        responses.add(
            responses.POST,
            "https://api.mock-shc.com/v1/patients",
            json={"error": "Malformed payload in batch 1"},
            status=400,
        )
        # Batch 2 returns 200 (pipeline must not halt!)
        responses.add(
            responses.POST,
            "https://api.mock-shc.com/v1/patients",
            json={"status": "ok"},
            status=200,
        )

        records = [
            {"patient_id": "P1"},
            {"patient_id": "P2"},
            {"patient_id": "P3"},
            {"patient_id": "P4"},
        ]

        summary = client.ingest_all(records)

        assert summary.total_batches == 2
        assert summary.successful_batches == 1
        assert summary.failed_batches == 1
        assert summary.records_uploaded == 2
        assert summary.records_failed == 2
        assert len(responses.calls) == 2
