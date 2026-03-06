# ==============================================================================
# Dockerfile — finpipe Data Engineering Pipeline
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Multi-stage builds — smaller final image (no build tools in prod)
#   2. Layer caching — requirements.txt changes rarely, code changes often
#   3. Non-root user — security best practice (never run as root in prod)
#   4. .env at runtime — secrets stay OUT of the image
#
# BUILD:
#   docker build -t finpipe .
#
# RUN (pass .env file for Snowflake credentials):
#   docker run --env-file .env finpipe health
#   docker run --env-file .env finpipe ingest --symbols TCS.NS --period 5d
#   docker run --env-file .env finpipe query "SELECT COUNT(*) FROM fact_stock_prices"
#
# ==============================================================================

# --- Stage 1: Builder (install dependencies with build tools) ----------------
FROM python:3.12-slim AS builder

WORKDIR /build

# Install build dependencies needed by snowflake-connector-python
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ libffi-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements first — this layer is cached unless requirements change
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# --- Stage 2: Runtime (lean image, no compilers) ----------------------------
FROM python:3.12-slim

LABEL maintainer="Aditya"
LABEL description="finpipe — Financial Data Engineering Pipeline"

# Create non-root user for security
RUN groupadd --gid 1000 finpipe && \
    useradd --uid 1000 --gid finpipe --create-home finpipe

WORKDIR /app

# Copy installed packages from builder stage
COPY --from=builder /install /usr/local

# Copy application code
COPY finpipe/ ./finpipe/

# Switch to non-root user
USER finpipe

# Default entrypoint — run the CLI
ENTRYPOINT ["python", "-m", "finpipe"]

# Default command (show help if no args given)
CMD ["--help"]
