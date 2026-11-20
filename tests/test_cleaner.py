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
        assert parse_account_balance("$-15.00") == -15.00
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

    def test_recover_unquoted_comma_balance_exact_sample(self, tmp_path: Path) -> None:
        """Verify safe recovery of malformed unquoted 1,250.00 row to float 1250.00."""
        csv_content = (
            "patient_id,first_name,last_name,dob,email,phone,account_balance,last_modified\n"
            "P-1002,Jane,Smith,12/31/1992,,,1,250.00,2026-10-02T11:30:00Z\n"
        )
        test_file = tmp_path / "malformed_balance.csv"
        test_file.write_text(csv_content, encoding="utf-8")

        records, report = read_and_clean_patient_data(test_file)

        assert report.total_rows_read == 1
        assert report.malformed_rows_recovered == 1
        assert report.malformed_rows_rejected == 0
        assert len(records) == 1

        recovered = records[0]
        assert recovered.patient_id == "P-1002"
        assert recovered.first_name == "Jane"
        assert recovered.last_name == "Smith"
        assert recovered.dob == "1992-12-31"
        assert recovered.email == ""
        assert recovered.phone == ""
        # Strictly verify that account balance became 1250.00 float
        assert recovered.account_balance == 1250.00
        assert recovered.last_modified == "2026-10-02T11:30:00+00:00"

    def test_ambiguous_malformed_row_quarantined_not_guessed(self, tmp_path: Path) -> None:
        """Verify that when recovery is ambiguous (e.g. invalid final timestamp), record is quarantined."""
        csv_content = (
            "patient_id,first_name,last_name,dob,email,phone,account_balance,last_modified\n"
            "P-9999,Bad,Row,1990-01-01,bad@example.com,555-0000,1,250.00,NOT_A_TIMESTAMP\n"
        )
        test_file = tmp_path / "ambiguous.csv"
        test_file.write_text(csv_content, encoding="utf-8")

        records, report = read_and_clean_patient_data(test_file)

        assert report.total_rows_read == 1
        assert report.malformed_rows_recovered == 0
        assert report.malformed_rows_rejected == 1
        assert len(records) == 0

    def test_read_original_shc_sample_pdf_dataset(self) -> None:
        """Verify full ingestion of the exact PDF sample dataset."""
        sample_path = Path("data/patients_shc_sample.csv")
        assert sample_path.exists()

        records, report = read_and_clean_patient_data(sample_path)

        assert report.total_rows_read == 9
        # P-1002 safely recovered
        assert report.malformed_rows_recovered == 1
        assert report.malformed_rows_rejected == 0
        # P-1001 and P-1003 deduplicated
        assert report.duplicates_removed == 2
        # Final unique patients: P-1001, P-1002, P-1003, P-1004, P-1005, P-1006, P-1007
        assert report.final_clean_count == 7
        assert len(records) == 7

        # Verify P-1002 recovered balance
        p1002 = next(r for r in records if r.patient_id == "P-1002")
        assert p1002.account_balance == 1250.00

        # Verify P-1007 negative balance $-15.00
        p1007 = next(r for r in records if r.patient_id == "P-1007")
        assert p1007.account_balance == -15.00

        # Verify P-1001 retained newer record (0.00 from 2026-10-05)
        p1001 = next(r for r in records if r.patient_id == "P-1001")
        assert p1001.account_balance == 0.00
        assert p1001.email == "john.doe@newemail.com"

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
        assert "patient_id,first_name,last_name,dob,email,phone,account_balance,last_modified" in content
        assert "P500,Diana,Prince,1984-01-01,diana@example.com,555-9999,1250.5,2024-01-01T00:00:00+00:00" in content
