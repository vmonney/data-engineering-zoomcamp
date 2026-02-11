"""02_docker_stats – Airflow equivalent of the Kestra '02_python' flow.

Kestra original:
  - Spins up a Docker container (python:slim)
  - Installs `requests` + `kestra` pip packages
  - Queries Docker Hub API for pull count of kestra/kestra
  - Pushes the result as a Kestra output + metric

Airflow translation:
  - Tasks run inside the Airflow worker (no extra container needed)
  - `requests` is installed via pyproject.toml
  - XCom replaces Kestra.outputs() for passing data between tasks
  - Airflow metrics/logging replaces Kestra metrics
"""

import logging
import time
from datetime import datetime

import requests
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)


@dag(
    dag_id="02_docker_stats",
    description=(
        "Queries the Docker Hub API to get the number of downloads "
        "for the Kestra Docker image, then logs and returns the result."
    ),
    schedule=None,                   # manual trigger only (like the Kestra flow)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["zoomcamp"],
)
def docker_stats():
    # ── Step 1: Collect stats from Docker Hub ────────────────────────────
    @task
    def collect_stats(image_name: str = "kestra/kestra") -> dict:
        """Query Docker Hub API and return download count.

        This is the direct translation of the Kestra `collect_stats` task.
        Instead of `Kestra.outputs(outputs)`, we simply *return* a dict;
        Airflow serialises it into XCom automatically.
        """
        url = f"https://hub.docker.com/v2/repositories/{image_name}/"
        start = time.time()

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()
        downloads = data.get("pull_count", "Not available")
        duration = round(time.time() - start, 3)

        logger.info("Docker image '%s' has %s downloads", image_name, downloads)
        logger.info("API call took %s seconds", duration)

        # This dict is the Airflow equivalent of Kestra.outputs(outputs)
        return {"downloads": downloads, "duration_seconds": duration}

    # ── Step 2: Log / display the output ─────────────────────────────────
    @task
    def log_results(stats: dict):
        """Read the stats pushed by collect_stats (via XCom) and log them.

        This is an extra task to show how XCom works — the downstream task
        receives the upstream return value as a plain Python dict.
        """
        logger.info(
            "Kestra Docker image has been downloaded %s times (fetched in %ss)",
            stats["downloads"],
            stats["duration_seconds"],
        )

    # ── Wire the tasks together ──────────────────────────────────────────
    stats = collect_stats()
    log_results(stats)


# Instantiate the DAG
docker_stats()
