"""Data Extraction, Validation, and Normalization Module.

This module processes raw legacy patient CSV exports, cleans and normalizes
fields (DOB to ISO 8601, currency strings to float, handling missing contact info),
validates mandatory fields, deduplicates records by patient_id based on the latest
last_modified timestamp, and exports cleaned data to CSV.
"""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import logging
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("shc_ingestion.cleaner")

# Regex pattern for currency symbols and extraneous whitespace
CURRENCY_SYMBOLS_PATTERN = re.compile(r"[\$€₹£\s]")

# Expected CSV column headers
EXPECTED_HEADERS = [
    "patient_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "dob",
    "account_balance",
    "last_modified",
]


@dataclass
class PatientRecord:
    """Represents a normalized patient record."""

    patient_id: str
    first_name: str
    last_name: str
    email: str
    phone: str
    dob: str
    account_balance: float
    last_modified: str
    _last_modified_dt: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert record to dictionary, omitting internal datetime metadata."""
        data = asdict(self)
        data.pop("_last_modified_dt", None)
        return data


@dataclass
class CleaningReport:
    """Contains statistical summary of the data cleaning process."""

    total_rows_read: int = 0
    valid_records_parsed: int = 0
    malformed_rows_rejected: int = 0
    invalid_records_rejected: int = 0
    duplicates_removed: int = 0
    final_clean_count: int = 0
    rejection_reasons: Dict[str, int] = None

    def __post_init__(self) -> None:
        if self.rejection_reasons is None:
            self.rejection_reasons = {}


def parse_and_normalize_dob(dob_raw: str) -> str:
    """Normalize date of birth string into ISO 8601 (YYYY-MM-DD) format.

    Supported input formats:
      - MM/DD/YYYY or M/D/YYYY (e.g. '04/15/1985')
      - YYYY-MM-DD (e.g. '1990-11-23')
      - DD-MM-YYYY or D-M-YYYY (e.g. '23-08-1976')

    Args:
        dob_raw: Raw date of birth string.

    Returns:
        Formatted date string as 'YYYY-MM-DD'.

    Raises:
        ValueError: If dob_raw is empty, malformed, or an invalid calendar date.
    """
    if not dob_raw or not isinstance(dob_raw, str):
        raise ValueError("DOB value is empty or not a string")

    cleaned_str = dob_raw.strip()
    if not cleaned_str:
        raise ValueError("DOB value is blank")

    # Format 1: YYYY-MM-DD
    if re.match(r"^\d{4}-\d{1,2}-\d{1,2}$", cleaned_str):
        try:
            dt = datetime.strptime(cleaned_str, "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d")
        except ValueError as err:
            raise ValueError(f"Invalid calendar date for YYYY-MM-DD '{cleaned_str}': {err}") from err

    # Format 2: MM/DD/YYYY or M/D/YYYY
    if re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", cleaned_str):
        try:
            dt = datetime.strptime(cleaned_str, "%m/%d/%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError as err:
            raise ValueError(f"Invalid calendar date for MM/DD/YYYY '{cleaned_str}': {err}") from err

    # Format 3: DD-MM-YYYY or D-M-YYYY
    if re.match(r"^\d{1,2}-\d{1,2}-\d{4}$", cleaned_str):
        try:
            dt = datetime.strptime(cleaned_str, "%d-%m-%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError as err:
            raise ValueError(f"Invalid calendar date for DD-MM-YYYY '{cleaned_str}': {err}") from err

    raise ValueError(
        f"Unsupported DOB format '{cleaned_str}'. Expected MM/DD/YYYY, YYYY-MM-DD, or DD-MM-YYYY."
    )


def parse_account_balance(balance_raw: Any) -> float:
    """Parse and convert account balance string into a float value.

    Handles:
      - Currency symbols: $, €, ₹, £
      - Comma separators: e.g. '1,250.00' -> 1250.00
      - Negative balances: '-$50.00', '$-50.00', '-50.00', or accounting '($50.00)'

    Args:
        balance_raw: Raw balance representation (str, float, int).

    Returns:
        Balance converted to a float.

    Raises:
        ValueError: If balance is empty or cannot be converted to float.
    """
    if balance_raw is None:
        raise ValueError("Account balance cannot be None")

    if isinstance(balance_raw, (int, float)):
        return round(float(balance_raw), 2)

    val_str = str(balance_raw).strip()
    if not val_str:
        raise ValueError("Account balance string is empty")

    is_negative = False

    # Check for accounting parentheses e.g. ($120.50) or (120.50)
    if val_str.startswith("(") and val_str.endswith(")"):
        is_negative = True
        val_str = val_str[1:-1].strip()

    # Check for minus sign
    if "-" in val_str:
        is_negative = True
        val_str = val_str.replace("-", "").strip()

    # Remove currency symbols and extraneous whitespace
    val_str = CURRENCY_SYMBOLS_PATTERN.sub("", val_str)

    # Remove comma thousand separators
    val_str = val_str.replace(",", "").strip()

    try:
        numeric_val = float(val_str)
    except ValueError as err:
        raise ValueError(f"Failed to parse numeric value from account balance '{balance_raw}': {err}") from err

    if is_negative:
        numeric_val = -abs(numeric_val)

    return round(numeric_val, 2)


def parse_last_modified_timestamp(ts_raw: str) -> Tuple[str, datetime]:
    """Parse and normalize ISO last_modified timestamp.

    Args:
        ts_raw: Raw timestamp string.

    Returns:
        Tuple of (ISO 8601 string, timezone-aware datetime object).

    Raises:
        ValueError: If timestamp is empty, invalid, or unparseable.
    """
    if not ts_raw or not isinstance(ts_raw, str):
        raise ValueError("last_modified timestamp is empty or invalid")

    cleaned_ts = ts_raw.strip()
    if not cleaned_ts:
        raise ValueError("last_modified timestamp is blank")

    # Normalize 'Z' to UTC offset '+00:00' for datetime.fromisoformat
    iso_candidate = cleaned_ts
    if iso_candidate.endswith("Z") or iso_candidate.endswith("z"):
        iso_candidate = iso_candidate[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(iso_candidate)
        # Ensure timezone awareness; default naive timestamps to UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat(), dt
    except ValueError as err:
        # Fallback parsing for common date-time strings
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(cleaned_ts, fmt).replace(tzinfo=timezone.utc)
                return dt.isoformat(), dt
            except ValueError:
                continue

        raise ValueError(f"Unable to parse last_modified timestamp '{cleaned_ts}': {err}") from err


def read_and_clean_patient_data(
    input_csv_path: str | Path,
) -> Tuple[List[PatientRecord], CleaningReport]:
    """Read raw patient CSV, validate fields, deduplicate, and return clean records.

    Handles malformed rows (such as unquoted commas in account_balance) by detecting
    column count mismatch, logging an explicit warning explaining the RFC 4180
    violation, and discarding the corrupt row to protect data integrity.

    Args:
        input_csv_path: Path to the raw CSV input file.

    Returns:
        Tuple of (list of cleaned, deduplicated PatientRecord instances, CleaningReport).
    """
    path = Path(input_csv_path)
    if not path.is_file():
        raise FileNotFoundError(f"Input patient file does not exist: {path.resolve()}")

    report = CleaningReport()
    records_by_id: Dict[str, List[PatientRecord]] = {}

    logger.info("Starting data extraction from %s", path)

    with open(path, mode="r", encoding="utf-8-sig", newline="") as csvfile:
        reader = csv.reader(csvfile)

        try:
            raw_headers = next(reader)
        except StopIteration:
            logger.warning("Input file %s is completely empty.", path)
            return [], report

        # Normalize header names (lowercase, stripped)
        headers = [h.strip().lower() for h in raw_headers]
        expected_len = len(headers)

        if "patient_id" not in headers or "last_modified" not in headers:
            logger.error("Mandatory columns missing from CSV header: %s", headers)
            raise ValueError(f"CSV header missing mandatory columns: {headers}")

        header_index = {name: idx for idx, name in enumerate(headers)}

        for line_number, row in enumerate(reader, start=2):
            report.total_rows_read += 1

            # Check for column count mismatch (e.g. malformed unquoted comma in balance)
            if len(row) != expected_len:
                report.malformed_rows_rejected += 1
                reason = "Column count mismatch (unquoted comma in field)"
                report.rejection_reasons[reason] = report.rejection_reasons.get(reason, 0) + 1

                # Detect specific unquoted currency comma issue
                row_preview = ",".join(row)
                if len(row) > expected_len and any("." in field and len(field) == 2 for field in row):
                    logger.warning(
                        "Line %d: Rejected malformed CSV row with %d columns (expected %d). "
                        "Suspected unquoted comma in account_balance (e.g. '1,250.00'). "
                        "Per RFC 4180, fields containing commas MUST be enclosed in double quotes. "
                        "Raw row: [%s]",
                        line_number,
                        len(row),
                        expected_len,
                        row_preview,
                    )
                else:
                    logger.warning(
                        "Line %d: Rejected malformed row with %d columns (expected %d). Content: %s",
                        line_number,
                        len(row),
                        expected_len,
                        row_preview,
                    )
                continue

            # Extract fields safely using header index
            raw_patient_id = row[header_index["patient_id"]].strip() if "patient_id" in header_index else ""
            raw_first_name = row[header_index["first_name"]].strip() if "first_name" in header_index else ""
            raw_last_name = row[header_index["last_name"]].strip() if "last_name" in header_index else ""
            raw_email = row[header_index["email"]].strip() if "email" in header_index else ""
            raw_phone = row[header_index["phone"]].strip() if "phone" in header_index else ""
            raw_dob = row[header_index["dob"]].strip() if "dob" in header_index else ""
            raw_balance = row[header_index["account_balance"]].strip() if "account_balance" in header_index else ""
            raw_last_modified = (
                row[header_index["last_modified"]].strip() if "last_modified" in header_index else ""
            )

            # Validation 1: Mandatory patient_id
            if not raw_patient_id:
                report.invalid_records_rejected += 1
                reason = "Missing mandatory field: patient_id"
                report.rejection_reasons[reason] = report.rejection_reasons.get(reason, 0) + 1
                logger.warning("Line %d: Rejected row missing mandatory 'patient_id'. Row: %s", line_number, row)
                continue

            # Validation 2: Mandatory last_modified
            if not raw_last_modified:
                report.invalid_records_rejected += 1
                reason = "Missing mandatory field: last_modified"
                report.rejection_reasons[reason] = report.rejection_reasons.get(reason, 0) + 1
                logger.warning(
                    "Line %d (patient_id=%s): Rejected row missing mandatory 'last_modified'.",
                    line_number,
                    raw_patient_id,
                )
                continue

            try:
                iso_ts, dt_ts = parse_last_modified_timestamp(raw_last_modified)
            except ValueError as err:
                report.invalid_records_rejected += 1
                reason = "Invalid last_modified timestamp"
                report.rejection_reasons[reason] = report.rejection_reasons.get(reason, 0) + 1
                logger.warning(
                    "Line %d (patient_id=%s): Rejected row with invalid last_modified '%s': %s",
                    line_number,
                    raw_patient_id,
                    raw_last_modified,
                    err,
                )
                continue

            # Validation 3: Date of Birth Normalization
            try:
                normalized_dob = parse_and_normalize_dob(raw_dob)
            except ValueError as err:
                report.invalid_records_rejected += 1
                reason = "Invalid or unparseable DOB"
                report.rejection_reasons[reason] = report.rejection_reasons.get(reason, 0) + 1
                logger.warning(
                    "Line %d (patient_id=%s): Rejected row with invalid DOB '%s': %s",
                    line_number,
                    raw_patient_id,
                    raw_dob,
                    err,
                )
                continue

            # Validation 4: Account Balance Conversion
            try:
                normalized_balance = parse_account_balance(raw_balance)
            except ValueError as err:
                report.invalid_records_rejected += 1
                reason = "Invalid account_balance format"
                report.rejection_reasons[reason] = report.rejection_reasons.get(reason, 0) + 1
                logger.warning(
                    "Line %d (patient_id=%s): Rejected row with invalid account_balance '%s': %s",
                    line_number,
                    raw_patient_id,
                    raw_balance,
                    err,
                )
                continue

            # Missing email and phone are allowed without crashing (clean default empty string)
            record = PatientRecord(
                patient_id=raw_patient_id,
                first_name=raw_first_name,
                last_name=raw_last_name,
                email=raw_email,
                phone=raw_phone,
                dob=normalized_dob,
                account_balance=normalized_balance,
                last_modified=iso_ts,
                _last_modified_dt=dt_ts,
            )

            report.valid_records_parsed += 1

            if raw_patient_id not in records_by_id:
                records_by_id[raw_patient_id] = []
            records_by_id[raw_patient_id].append(record)

    # Deduplication Step: Keep record with latest last_modified timestamp
    final_cleaned_records: List[PatientRecord] = []

    for patient_id, group in records_by_id.items():
        if len(group) == 1:
            final_cleaned_records.append(group[0])
        else:
            # Sort descending by timestamp
            group.sort(key=lambda r: r._last_modified_dt, reverse=True)
            chosen = group[0]
            dropped_count = len(group) - 1
            report.duplicates_removed += dropped_count

            logger.info(
                "Deduplicated patient_id=%s: Retained latest record (timestamp: %s); discarded %d older record(s).",
                patient_id,
                chosen.last_modified,
                dropped_count,
            )
            final_cleaned_records.append(chosen)

    # Sort final records by patient_id for deterministic output
    final_cleaned_records.sort(key=lambda r: r.patient_id)
    report.final_clean_count = len(final_cleaned_records)

    logger.info(
        "Data cleaning finished: Read=%d, Valid=%d, MalformedRejected=%d, "
        "InvalidRejected=%d, DuplicatesPruned=%d, FinalClean=%d",
        report.total_rows_read,
        report.valid_records_parsed,
        report.malformed_rows_rejected,
        report.invalid_records_rejected,
        report.duplicates_removed,
        report.final_clean_count,
    )

    return final_cleaned_records, report


def save_cleaned_patients_to_csv(
    records: List[PatientRecord],
    output_csv_path: str | Path,
) -> Path:
    """Save cleaned patient records to output CSV.

    Args:
        records: List of normalized PatientRecord instances.
        output_csv_path: Target path for the output CSV.

    Returns:
        Resolved Path of written CSV.
    """
    out_path = Path(output_csv_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, mode="w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=EXPECTED_HEADERS)
        writer.writeheader()
        for record in records:
            writer.writerow(record.to_dict())

    logger.info("Saved %d cleaned patient records to %s", len(records), out_path.resolve())
    return out_path.resolve()
