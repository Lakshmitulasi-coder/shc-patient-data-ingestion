"""Unit tests for Data Extraction, Validation, and Normalization (Task 1)."""

from datetime import datetime
from pathlib import Path
import pytest

from src.cleaner import (
    PatientRecord,
    parse_account_balance,
    parse_and_normalize_dob,
    parse_last_modified_timestamp,
    read_and_clean_patient_data,
    save_cleaned_patients_to_csv,
)


class TestDateOfBirthParsing:
    """Test suite for DOB normalization to ISO 8601 (YYYY-MM-DD)."""

    def test_iso_format_yyyy_mm_dd(self) -> None:
        assert parse_and_normalize_dob("1990-11-23") == "1990-11-23"
        assert parse_and_normalize_dob("2000-01-05") == "2000-01-05"

    def test_us_format_mm_dd_yyyy(self) -> None:
        assert parse_and_normalize_dob("04/15/1985") == "1985-04-15"
        assert parse_and_normalize_dob("12/30/1995") == "1995-12-30"
        assert parse_and_normalize_dob("4/5/1985") == "1985-04-05"

    def test_eu_format_dd_mm_yyyy(self) -> None:
        assert parse_and_normalize_dob("23-08-1976") == "1976-08-23"
        assert parse_and_normalize_dob("07-04-1988") == "1988-04-07"
        assert parse_and_normalize_dob("5-8-1976") == "1976-08-05"

    def test_invalid_calendar_dates_raise_value_error(self) -> None:
        # February 31 does not exist
        with pytest.raises(ValueError, match="Invalid calendar date"):
            parse_and_normalize_dob("1985-02-31")

        with pytest.raises(ValueError, match="Invalid calendar date"):
            parse_and_normalize_dob("02/30/1990")

        with pytest.raises(ValueError, match="Invalid calendar date"):
            parse_and_normalize_dob("31-04-2000")  # April has 30 days

    def test_unsupported_or_empty_formats_raise_value_error(self) -> None:
        with pytest.raises(ValueError):
            parse_and_normalize_dob("")

        with pytest.raises(ValueError):
            parse_and_normalize_dob("not-a-date")

        with pytest.raises(ValueError):
            parse_and_normalize_dob("1990.12.25")


class TestAccountBalanceParsing:
    """Test suite for currency normalization to float."""

    def test_plain_numbers(self) -> None:
        assert parse_account_balance("150.00") == 150.00
        assert parse_account_balance("0") == 0.0
        assert parse_account_balance(250.75) == 250.75
        assert parse_account_balance(100) == 100.0

    def test_currency_symbols_removal(self) -> None:
        assert parse_account_balance("$150.00") == 150.00
        assert parse_account_balance("€350.75") == 350.75
        assert parse_account_balance("₹15000.00") == 15000.00
        assert parse_account_balance("£85.20") == 85.20

    def test_comma_separated_values(self) -> None:
        assert parse_account_balance("1,250.00") == 1250.00
        assert parse_account_balance("$1,250.50") == 1250.50
        assert parse_account_balance("€2,450.00") == 2450.00
        assert parse_account_balance("₹1,50,000.00") == 150000.00
        assert parse_account_balance("12,345,678.90") == 12345678.90

    def test_negative_balances(self) -> None:
        assert parse_account_balance("-50.00") == -50.00
        assert parse_account_balance("-$50.00") == -50.00
        assert parse_account_balance("$-50.00") == -50.00
        assert parse_account_balance("-€75.25") == -75.25
        assert parse_account_balance("-₹500.00") == -500.00

    def test_accounting_parentheses_negative_balances(self) -> None:
        assert parse_account_balance("($120.50)") == -120.50
        assert parse_account_balance("(120.50)") == -120.50
        assert parse_account_balance("(€1,250.00)") == -1250.00

    def test_invalid_balances_raise_value_error(self) -> None:
        with pytest.raises(ValueError):
            parse_account_balance("NOT_A_NUMBER")

        with pytest.raises(ValueError):
            parse_account_balance("")

        with pytest.raises(ValueError):
            parse_account_balance(None)


class TestTimestampParsing:
    """Test suite for last_modified timestamp parsing."""

    def test_valid_iso_timestamps(self) -> None:
        ts_str, dt = parse_last_modified_timestamp("2023-10-01T10:00:00Z")
        assert ts_str == "2023-10-01T10:00:00+00:00"
        assert isinstance(dt, datetime)

        ts_str, dt = parse_last_modified_timestamp("2024-01-05T09:00:00+00:00")
        assert ts_str == "2024-01-05T09:00:00+00:00"

    def test_invalid_timestamps_raise_value_error(self) -> None:
        with pytest.raises(ValueError):
            parse_last_modified_timestamp("")

        with pytest.raises(ValueError):
            parse_last_modified_timestamp("invalid-date")


class TestPatientDataIngestionAndDeduplication:
    """Test suite for CSV ingestion, validation, and deduplication logic."""

    def test_read_sample_csv_with_all_cases(self, tmp_path: Path) -> None:
        csv_content = (
            "patient_id,first_name,last_name,email,phone,dob,account_balance,last_modified\n"
            "P001,John,Doe,john@example.com,555-0101,04/15/1985,$150.00,2023-10-01T10:00:00Z\n"
            "P001,John,Doe,john@example.com,555-0101,04/15/1985,$200.50,2023-11-15T14:30:00Z\n"
            "P002,Jane,Smith,,555-0102,1990-11-23,€350.75,2023-10-05T08:15:00Z\n"
            "P003,Robert,Johnson,robert@example.com,,23-08-1976,₹15000.00,2024-01-05T09:00:00Z\n"
            "P004,Emily,Davis,,,12/30/1995,-$50.00,2023-11-20T16:45:00Z\n"
            'P006,Sarah,Wilson,sarah@example.com,555-0106,07-04-1988,"1,250.00",2023-12-10T15:00:00Z\n'
            "P009,Frank,Miller,frank@example.com,555-0109,1983-07-22,1,250.00,2023-12-05T11:00:00Z\n"
            ",Missing,ID,missing@example.com,555-0000,1990-01-01,100.00,2023-12-01T10:00:00Z\n"
            "P011,David,NoTimestamp,david@example.com,555-0111,1988-03-15,$250.00,\n"
            "P012,Eva,BadDate,eva@example.com,555-0112,1985-02-31,$300.00,2023-12-01T10:00:00Z\n"
        )
        test_file = tmp_path / "test_patients.csv"
        test_file.write_text(csv_content, encoding="utf-8")

        records, report = read_and_clean_patient_data(test_file)

        # Total rows read: 10
        assert report.total_rows_read == 10
        # P009 has unquoted 1,250.00 resulting in 9 columns -> rejected as malformed
        assert report.malformed_rows_rejected == 1
        # Missing ID, missing timestamp, bad date (Feb 31) -> 3 rejected
        assert report.invalid_records_rejected == 3
        # Valid records parsed: P001 (x2), P002, P003, P004, P006 = 6
        assert report.valid_records_parsed == 6
        # P001 is duplicate, older one pruned -> 1 duplicate removed
        assert report.duplicates_removed == 1
        # Final clean records: P001, P002, P003, P004, P006 = 5
        assert report.final_clean_count == 5
        assert len(records) == 5

    def test_duplicate_deduplication_keeps_latest_timestamp(self, tmp_path: Path) -> None:
        csv_content = (
            "patient_id,first_name,last_name,email,phone,dob,account_balance,last_modified\n"
            "DUP1,Early,Record,early@example.com,555-0001,1980-01-01,$10.00,2023-01-01T00:00:00Z\n"
            "DUP1,Late,Record,late@example.com,555-0002,1980-01-01,$99.00,2023-12-31T23:59:59Z\n"
            "DUP1,Middle,Record,mid@example.com,555-0003,1980-01-01,$50.00,2023-06-15T12:00:00Z\n"
        )
        test_file = tmp_path / "dup_test.csv"
        test_file.write_text(csv_content, encoding="utf-8")

        records, report = read_and_clean_patient_data(test_file)

        assert report.duplicates_removed == 2
        assert len(records) == 1
        kept = records[0]
        assert kept.first_name == "Late"
        assert kept.account_balance == 99.00
        assert kept.last_modified == "2023-12-31T23:59:59+00:00"

    def test_missing_email_and_phone_handled_safely(self, tmp_path: Path) -> None:
        csv_content = (
            "patient_id,first_name,last_name,email,phone,dob,account_balance,last_modified\n"
            "P100,Alice,Smith,,,1992-05-10,$0.00,2023-10-10T10:00:00Z\n"
        )
        test_file = tmp_path / "missing_contact.csv"
        test_file.write_text(csv_content, encoding="utf-8")

        records, report = read_and_clean_patient_data(test_file)

        assert len(records) == 1
        assert records[0].email == ""
        assert records[0].phone == ""

    def test_save_cleaned_patients_to_csv(self, tmp_path: Path) -> None:
        record = PatientRecord(
            patient_id="P500",
            first_name="Diana",
            last_name="Prince",
            email="diana@example.com",
            phone="555-9999",
            dob="1984-01-01",
            account_balance=1250.50,
            last_modified="2024-01-01T00:00:00+00:00",
        )
        output_file = tmp_path / "subfolder" / "cleaned_output.csv"
        saved_path = save_cleaned_patients_to_csv([record], output_file)

        assert saved_path.exists()
        content = saved_path.read_text(encoding="utf-8")
        assert "patient_id,first_name,last_name,email,phone,dob,account_balance,last_modified" in content
        assert "P500,Diana,Prince,diana@example.com,555-9999,1984-01-01,1250.5,2024-01-01T00:00:00+00:00" in content
