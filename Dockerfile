# syntax=docker/dockerfile:1
# Multi-stage Dockerfile for SHC Patient Data Ingestion Engine

# ------------------------------------------------------------------------------
# Stage 1: Build & Dependency Resolution
# ------------------------------------------------------------------------------
FROM python:3.12-slim AS builder

WORKDIR /build

# Install security updates and build requirements
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Create isolated Python virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install application dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# ------------------------------------------------------------------------------
# Stage 2: Minimal Production Runtime
# ------------------------------------------------------------------------------
FROM python:3.12-slim AS runner

# Metadata labels
LABEL maintainer="SHC Forward Deployed Engineering Team" \
      version="1.0.0" \
      description="Production Ingestion & Fault-Tolerant API Engine for Legacy Patient Data"

# Runtime environment settings
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    API_BASE_URL="https://api.mock-shc.com/v1/patients" \
    API_TOKEN="" \
    INPUT_FILE="data/patients.csv" \
    OUTPUT_FILE="data/cleaned_patients.csv" \
    FAILED_RECORDS_FILE="failed_records.json" \
    BATCH_SIZE="100" \
    MAX_RETRIES="3" \
    RATE_LIMIT_RPS="5.0" \
    LOG_LEVEL="INFO"

# Create unprivileged group and user (non-root)
RUN groupadd --gid 1001 appgroup && \
    useradd --uid 1001 --gid appgroup --shell /bin/bash --create-home appuser

WORKDIR /app

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Copy source code and sample data
COPY src/ /app/src/
COPY data/ /app/data/

# Initialize failure tracking file and set directory permissions for non-root user
RUN echo "[]" > /app/failed_records.json && \
    chown -R appuser:appgroup /app && \
    chmod -R 775 /app/data

# Declare mount point for host filesystem data persistence
VOLUME ["/app/data"]

# Switch to non-root user
USER appuser

# Healthcheck to verify Python runtime
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import src.cleaner, src.api_client; print('OK')" || exit 1

# Default execution entrypoint
ENTRYPOINT ["python", "-m", "src.main"]
CMD []
