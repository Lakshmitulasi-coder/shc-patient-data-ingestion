"""Main Pipeline Orchestrator for SHC Patient Data Ingestion.

Coordinates:
  1. Environment configuration and structured logging setup.
  2. Data extraction, validation, and normalization (Task 1).
  3. Exporting cleaned data to CSV (Task 1).
  4. Fault-tolerant API ingestion with rate limiting and exponential backoff (Task 2).
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
import sys
from typing import Optional

from dotenv import load_dotenv

# Ensure project root is in sys.path when executed directly as a script
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.api_client import SHCPatientAPIClient
from src.cleaner import read_and_clean_patient_data, save_cleaned_patients_to_csv


def setup_logging(log_level_str: Optional[str] = None) -> logging.Logger:
    """Configure structured console logging.

    Args:
        log_level_str: String log level (DEBUG, INFO, WARNING, ERROR).

    Returns:
        Configured root logger.
    """
    level_name = (log_level_str or os.getenv("LOG_LEVEL", "INFO")).upper()
    numeric_level = getattr(logging, level_name, logging.INFO)

    log_format = "%(asctime)s [%(levelname)s] [%(name)s]: %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=numeric_level,
        format=log_format,
        datefmt=date_format,
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )

    root_logger = logging.getLogger("shc_ingestion")
    root_logger.setLevel(numeric_level)
    return root_logger


def parse_arguments() -> argparse.Namespace:
    """Parse command line options with fallback to environment variables."""
    parser = argparse.ArgumentParser(
        description="SHC Patient Data Ingestion & Fault-Tolerant API Transmission Engine"
    )

    parser.add_argument(
        "--input",
        "-i",
        dest="input_file",
        default=os.getenv("INPUT_FILE", "data/patients_shc_sample.csv"),
        help="Path to raw input patient CSV file (default: data/patients_shc_sample.csv)",
    )
    parser.add_argument(
        "--output",
        "-o",
        dest="output_file",
        default=os.getenv("OUTPUT_FILE", "data/cleaned_patients.csv"),
        help="Path to output cleaned patient CSV file (default: data/cleaned_patients.csv)",
    )
    parser.add_argument(
        "--api-url",
        dest="api_base_url",
        default=os.getenv("API_BASE_URL", "https://api.mock-shc.com/v1/patients"),
        help="Target API endpoint URL",
    )
    parser.add_argument(
        "--token",
        dest="api_token",
        default=os.getenv("API_TOKEN", ""),
        help="Bearer authentication token",
    )
    parser.add_argument(
        "--batch-size",
        dest="batch_size",
        type=int,
        default=int(os.getenv("BATCH_SIZE", "100")),
        help="Maximum patient records per HTTP request (default: 100)",
    )
    parser.add_argument(
        "--max-retries",
        dest="max_retries",
        type=int,
        default=int(os.getenv("MAX_RETRIES", "3")),
        help="Maximum retries for 429/5xx/network errors (default: 3)",
    )
    parser.add_argument(
        "--rate-limit",
        dest="rate_limit_rps",
        type=float,
        default=float(os.getenv("RATE_LIMIT_RPS", "5.0")),
        help="Maximum API requests per second (default: 5.0)",
    )
    parser.add_argument(
        "--failed-records",
        dest="failed_records_file",
        default=os.getenv("FAILED_RECORDS_FILE", "failed_records.json"),
        help="Path to JSON file storing rejected HTTP 400 and failed records",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Execute data extraction, normalization, and CSV export without making API calls",
    )

    return parser.parse_args()


def run_pipeline() -> int:
    """Execute the end-to-end data ingestion pipeline.

    Returns:
        Exit code: 0 on complete success, 1 on critical pipeline failure.
    """
    # Load .env file if available
    load_dotenv(override=False)

    args = parse_arguments()
    logger = setup_logging()

    logger.info("================================================================================")
    logger.info("Starting SHC Patient Data Ingestion & API Transmission Pipeline")
    logger.info("================================================================================")
    logger.info("Configuration:")
    logger.info("  Input File:           %s", args.input_file)
    logger.info("  Output File:          %s", args.output_file)
    logger.info("  API Endpoint:         %s", args.api_base_url)
    logger.info("  Batch Size:           %d", args.batch_size)
    logger.info("  Max Retries:          %d", args.max_retries)
    logger.info("  Rate Limit:           %.1f req/sec", args.rate_limit_rps)
    logger.info("  Failed Records File:  %s", args.failed_records_file)
    logger.info("  Dry Run Mode:         %s", args.dry_run)
    logger.info("--------------------------------------------------------------------------------")

    # Step 1 & 2: Data Extraction, Normalization & CSV Export (Task 1)
    try:
        cleaned_records, report = read_and_clean_patient_data(args.input_file)
    except Exception as err:
        logger.exception("Fatal error during data extraction from %s: %s", args.input_file, err)
        return 1

    try:
        saved_csv_path = save_cleaned_patients_to_csv(cleaned_records, args.output_file)
    except Exception as err:
        logger.exception("Fatal error writing cleaned CSV to %s: %s", args.output_file, err)
        return 1

    logger.info("--------------------------------------------------------------------------------")
    logger.info("Task 1 Data Cleaning Summary:")
    logger.info("  Total Raw Rows Read:         %d", report.total_rows_read)
    logger.info("  Valid Records Parsed:        %d", report.valid_records_parsed)
    logger.info("  Malformed Rows Rejected:     %d", report.malformed_rows_rejected)
    logger.info("  Invalid Records Rejected:    %d", report.invalid_records_rejected)
    logger.info("  Duplicates Removed (by ID):  %d", report.duplicates_removed)
    logger.info("  Final Clean Records Saved:   %d (to %s)", report.final_clean_count, saved_csv_path)

    if report.rejection_reasons:
        logger.info("  Rejection Reasons Breakdown:")
        for reason, count in report.rejection_reasons.items():
            logger.info("    - %s: %d", reason, count)

    if args.dry_run:
        logger.info("--------------------------------------------------------------------------------")
        logger.info("Dry-run specified. Skipping API transmission phase.")
        logger.info("================================================================================")
        return 0

    # Step 3: API Ingestion and Fault-Tolerant Transmission (Task 2)
    logger.info("--------------------------------------------------------------------------------")
    logger.info("Task 2 API Transmission Phase:")

    if not cleaned_records:
        logger.warning("No cleaned records available for API transmission. Pipeline completed.")
        return 0

    api_client = SHCPatientAPIClient(
        base_url=args.api_base_url,
        api_token=args.api_token,
        batch_size=args.batch_size,
        rate_limit_rps=args.rate_limit_rps,
        max_retries=args.max_retries,
        failed_records_file=args.failed_records_file,
    )

    ingestion_summary = api_client.ingest_all(cleaned_records)

    logger.info("--------------------------------------------------------------------------------")
    logger.info("Ingestion Execution Summary:")
    logger.info("  Total Patient Records:  %d", ingestion_summary.total_records)
    logger.info("  Total Batches:          %d", ingestion_summary.total_batches)
    logger.info("  Successful Batches:     %d", ingestion_summary.successful_batches)
    logger.info("  Failed Batches:         %d", ingestion_summary.failed_batches)
    logger.info("  Records Uploaded:       %d", ingestion_summary.records_uploaded)
    logger.info("  Records Failed:         %d", ingestion_summary.records_failed)

    if ingestion_summary.records_failed > 0:
        logger.warning(
            "Notice: %d records failed transmission. Details stored in %s.",
            ingestion_summary.records_failed,
            Path(args.failed_records_file).resolve(),
        )

    logger.info("================================================================================")
    logger.info("Pipeline execution complete.")
    logger.info("================================================================================")
    return 0


if __name__ == "__main__":
    sys.exit(run_pipeline())
