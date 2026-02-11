"""01_hello_world – Airflow equivalent of the Kestra zoomcamp hello-world flow."""

import logging
import time
from datetime import datetime, timedelta

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

# ── Equivalent of Kestra `pluginDefaults` (log level → ERROR) ──────────────
logger.setLevel(logging.ERROR)


@dag(
    dag_id="01_hello_world",
    # ── schedule (disabled) ─ equivalent of triggers.schedule ──────────────
    schedule="0 10 * * *",          # cron: every day at 10:00 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,   # equivalent of `disabled: true`
    # ── inputs ─────────────────────────────────────────────────────────────
    params={"name": "Will"},        # default input, overridable at trigger time
    # ── concurrency ────────────────────────────────────────────────────────
    max_active_runs=2,              # equivalent of concurrency.limit: 2
    tags=["zoomcamp"],              # equivalent of namespace
)
def hello_world():
    # ── variables ──────────────────────────────────────────────────────────
    # In Airflow, Jinja templates are rendered inside operator fields.
    # For the TaskFlow API we use `params` dict directly in Python.

    @task
    def hello_message(**context):
        name = context["params"]["name"]
        welcome_message = f"Hello, {name}!"
        logger.error(welcome_message)  # ERROR level to match pluginDefaults

    @task
    def generate_output(**context):
        """Equivalent of Kestra's debug.Return – pushes a value via XCom."""
        return "I was generated during this workflow."

    @task
    def sleep_task():
        """Equivalent of Kestra's flow.Sleep (PT15S = 15 seconds)."""
        logger.error("Sleeping for 15 seconds…")
        time.sleep(15)

    @task
    def log_output(generated_value: str):
        """Reads the output from generate_output (pulled automatically via XCom)."""
        logger.error(f"This is an output: {generated_value}")

    @task
    def goodbye_message(**context):
        name = context["params"]["name"]
        logger.error(f"Goodbye, {name}!")

    # ── task dependencies (sequential, like Kestra's default) ──────────────
    hello = hello_message()
    output = generate_output()
    slp = sleep_task()
    log = log_output(output)  # passing XCom value directly
    goodbye = goodbye_message()

    hello >> output >> slp >> log >> goodbye


# Instantiate the DAG
hello_world()