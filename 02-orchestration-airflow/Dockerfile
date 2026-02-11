FROM apache/airflow:2.10.4-python3.12

USER root

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install project dependencies into Airflow's venv
RUN uv pip install --system --no-cache -r pyproject.toml

# Copy your pipeline scripts so DAGs can import them
COPY ingest_data.py /opt/airflow/scripts/ingest_data.py

USER airflow