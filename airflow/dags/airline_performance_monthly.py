from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from airline_warehouse.config import Settings
from airline_warehouse.downloader import BtsDownloader
from airline_warehouse.month import YearMonth
from airline_warehouse.validation import validate_raw_file as validate_file
from airline_warehouse.warehouse import SnowflakeWarehouse

DEFAULT_ARGS = {
    "owner": "Aneesh Mokashi",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _settings() -> Settings:
    return Settings.from_env()


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _resolve_source_month(context: dict[str, Any]) -> YearMonth:
    configured = str(context["params"]["source_month"]).strip().upper()
    if configured != "AUTO":
        return YearMonth.parse(configured)
    interval_end = context["data_interval_end"]
    year = interval_end.year
    month = interval_end.month - 2
    while month <= 0:
        year -= 1
        month += 12
    return YearMonth(year, month)


def _task_failure(context: dict[str, Any]) -> None:
    task_instance = context["task_instance"]
    state = task_instance.xcom_pull(task_ids="check_month")
    if not isinstance(state, dict) or not state.get("run_id"):
        return
    exception = context.get("exception")
    message = f"{task_instance.task_id}: {exception or 'Airflow task failed'}"
    SnowflakeWarehouse(_settings()).fail_run(
        state["run_id"], datetime.now(timezone.utc), message
    )


@dag(
    dag_id="airline_performance_monthly",
    description="Incrementally ingest one BTS reporting-carrier month into Snowflake",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule="0 6 15 * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"source_month": "AUTO", "force": False},
    on_failure_callback=_task_failure,
    tags=["bts", "snowflake", "incremental"],
)
def airline_performance_monthly() -> None:
    @task.short_circuit(task_id="check_month")
    def check_month() -> dict[str, Any] | bool:
        context = get_current_context()
        month = _resolve_source_month(context)
        force = bool(context["params"].get("force", False))
        warehouse = SnowflakeWarehouse(_settings())
        if not force and warehouse.month_is_loaded(month):
            return False
        state = {
            "run_id": str(uuid4()),
            "source_month": str(month),
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        warehouse.begin_run(
            state["run_id"], month, _parse_timestamp(state["started_at"]), "INCREMENTAL", None
        )
        return state

    @task(task_id="download_data")
    def download_data(state: dict[str, Any]) -> dict[str, Any]:
        settings = _settings()
        month = YearMonth.parse(state["source_month"])
        started = datetime.now(timezone.utc)
        result = BtsDownloader(
            settings.bts_base_url, settings.raw_data_dir, settings.request_timeout_seconds
        ).download(month)
        return {
            **state,
            **result.as_dict(),
            "download_started_at": started.isoformat(),
            "download_completed_at": datetime.now(timezone.utc).isoformat(),
        }

    @task(task_id="validate_raw_file")
    def validate_raw_file(state: dict[str, Any]) -> dict[str, Any]:
        result = validate_file(
            Path(state["csv_path"]), YearMonth.parse(state["source_month"])
        )
        return {**state, "validation": result.as_dict()}

    @task(task_id="upload_to_snowflake_stage")
    def upload_to_snowflake_stage(state: dict[str, Any]) -> dict[str, Any]:
        started = datetime.now(timezone.utc)
        stage_prefix = SnowflakeWarehouse(_settings()).upload_file(
            Path(state["csv_path"]),
            YearMonth.parse(state["source_month"]),
            state["checksum_sha256"],
        )
        return {
            **state,
            "stage_prefix": stage_prefix,
            "load_started_at": started.isoformat(),
        }

    @task(task_id="load_raw")
    def load_raw(state: dict[str, Any]) -> dict[str, Any]:
        SnowflakeWarehouse(_settings()).load_raw(
            YearMonth.parse(state["source_month"]),
            Path(state["csv_path"]).name,
            state["checksum_sha256"],
            state["run_id"],
            state["stage_prefix"],
        )
        return {**state, "load_completed_at": datetime.now(timezone.utc).isoformat()}

    @task(task_id="transform_staging")
    def transform_staging(state: dict[str, Any]) -> dict[str, Any]:
        started = datetime.now(timezone.utc)
        SnowflakeWarehouse(_settings()).transform_staging(
            YearMonth.parse(state["source_month"])
        )
        return {**state, "transformation_started_at": started.isoformat()}

    @task(task_id="merge_fact_dimensions")
    def merge_fact_dimensions(state: dict[str, Any]) -> dict[str, Any]:
        SnowflakeWarehouse(_settings()).merge_dimensions_and_fact(
            YearMonth.parse(state["source_month"])
        )
        return state

    @task(task_id="build_analytics_marts")
    def build_analytics_marts(state: dict[str, Any]) -> dict[str, Any]:
        SnowflakeWarehouse(_settings()).rebuild_marts()
        return {
            **state,
            "transformation_completed_at": datetime.now(timezone.utc).isoformat(),
        }

    @task(task_id="validate_counts")
    def validate_counts(state: dict[str, Any]) -> dict[str, Any]:
        counts = SnowflakeWarehouse(_settings()).validate_month(
            state["run_id"],
            YearMonth.parse(state["source_month"]),
            int(state["validation"]["row_count"]),
        )
        return {**state, "counts": counts}

    @task(task_id="record_pipeline_metrics")
    def record_pipeline_metrics(state: dict[str, Any]) -> dict[str, Any]:
        completed_at = datetime.now(timezone.utc)
        download_seconds = (
            _parse_timestamp(state["download_completed_at"])
            - _parse_timestamp(state["download_started_at"])
        ).total_seconds()
        load_seconds = (
            _parse_timestamp(state["load_completed_at"])
            - _parse_timestamp(state["load_started_at"])
        ).total_seconds()
        transformation_seconds = (
            _parse_timestamp(state["transformation_completed_at"])
            - _parse_timestamp(state["transformation_started_at"])
        ).total_seconds()
        total_seconds = (
            completed_at - _parse_timestamp(state["started_at"])
        ).total_seconds()
        SnowflakeWarehouse(_settings()).complete_run(
            run_id=state["run_id"],
            month=YearMonth.parse(state["source_month"]),
            source_file=Path(state["csv_path"]).name,
            checksum=state["checksum_sha256"],
            source_file_bytes=int(state["csv_bytes"]),
            counts=state["counts"],
            download_seconds=download_seconds,
            load_seconds=load_seconds,
            transformation_seconds=transformation_seconds,
            total_seconds=total_seconds,
            completed_at=completed_at,
        )
        return {
            "run_id": state["run_id"],
            "source_month": state["source_month"],
            "status": "SUCCESS",
            "total_duration_seconds": total_seconds,
        }

    checked = check_month()
    downloaded = download_data(checked)
    validated = validate_raw_file(downloaded)
    uploaded = upload_to_snowflake_stage(validated)
    loaded = load_raw(uploaded)
    staged = transform_staging(loaded)
    merged = merge_fact_dimensions(staged)
    marts = build_analytics_marts(merged)
    counted = validate_counts(marts)
    record_pipeline_metrics(counted)


airline_performance_monthly()
