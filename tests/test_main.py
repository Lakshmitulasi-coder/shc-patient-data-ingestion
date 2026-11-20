"""Integration tests for the main orchestration pipeline."""

from pathlib import Path
import responses

from src.main import run_pipeline


class TestMainPipelineIntegration:
    """End-to-end integration tests for src.main."""

    def test_pipeline_dry_run_generates_cleaned_csv(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        input_csv = tmp_path / "patients.csv"
        output_csv = tmp_path / "cleaned_patients.csv"

        input_csv.write_text(
            "patient_id,first_name,last_name,email,phone,dob,account_balance,last_modified\n"
            "P001,John,Doe,john@example.com,555-0101,04/15/1985,$150.00,2023-10-01T10:00:00Z\n"
            "P001,John,Doe,john@example.com,555-0101,04/15/1985,$200.00,2023-11-15T14:30:00Z\n",
            encoding="utf-8",
        )

        test_args = [
            "main.py",
            "--input",
            str(input_csv),
            "--output",
            str(output_csv),
            "--dry-run",
        ]
        monkeypatch.setattr("sys.argv", test_args)

        exit_code = run_pipeline()
        assert exit_code == 0
        assert output_csv.exists()

        content = output_csv.read_text(encoding="utf-8")
        assert "P001,John,Doe" in content
        # Deduplication kept latest record: balance 200.0
        assert "200.0" in content

    @responses.activate
    def test_pipeline_full_run_with_mock_api(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        responses.add(
            responses.POST,
            "https://api.mock-shc.com/v1/patients",
            json={"status": "success", "processed": 1},
            status=200,
        )

        input_csv = tmp_path / "patients.csv"
        output_csv = tmp_path / "cleaned_patients.csv"
        failed_file = tmp_path / "failed.json"

        input_csv.write_text(
            "patient_id,first_name,last_name,email,phone,dob,account_balance,last_modified\n"
            "P002,Jane,Smith,jane@example.com,555-0102,1990-11-23,€350.75,2023-10-05T08:15:00Z\n",
            encoding="utf-8",
        )

        test_args = [
            "main.py",
            "--input",
            str(input_csv),
            "--output",
            str(output_csv),
            "--failed-records",
            str(failed_file),
            "--api-url",
            "https://api.mock-shc.com/v1/patients",
            "--token",
            "test-token",
        ]
        monkeypatch.setattr("sys.argv", test_args)

        exit_code = run_pipeline()
        assert exit_code == 0
        assert output_csv.exists()
        assert len(responses.calls) == 1
