FROM python:3.13-slim

RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /usr/src/app
ENV PATH="/usr/src/app/.venv/bin:$PATH"

COPY pyproject.toml .python-version ./
RUN uv sync

COPY profiles.yml /root/.dbt/profiles.yml
COPY scripts/ ./scripts/

WORKDIR /usr/src/app/taxi_rides_ny
