from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from airline_warehouse.tasks import AIRFLOW_TASK_IDS
from airline_warehouse.warehouse import SnowflakeWarehouse


def collect_metrics(warehouse: SnowflakeWarehouse) -> dict[str, Any]:
    settings = warehouse.settings
    database = settings.database
    analytics = settings.analytics_schema
    metadata = settings.metadata_schema

    warehouse_counts = warehouse.fetch_one(
        f"""
        SELECT
          (SELECT COUNT(*) FROM {database}.{analytics}.FACT_FLIGHTS),
          (SELECT COUNT(DISTINCT SOURCE_MONTH) FROM {database}.{analytics}.FACT_FLIGHTS),
          (SELECT COUNT(*) FROM {database}.{analytics}.DIM_AIRLINE),
          (SELECT COUNT(*) FROM {database}.{analytics}.DIM_AIRPORT),
          (SELECT COUNT(*) FROM {database}.{analytics}.DIM_DATE)
        """
    )
    measured_loads = warehouse.fetch_one(
        f"""
        SELECT
          COALESCE(SUM(R.SOURCE_FILE_BYTES), 0),
          COALESCE(SUM(R.DUPLICATE_COUNT), 0),
          COALESCE(SUM(R.REJECTED_RECORD_COUNT), 0),
          ROUND(AVG(R.TOTAL_DURATION_SECONDS), 3)
        FROM {database}.{metadata}.PIPELINE_LOADS L
        JOIN {database}.{metadata}.PIPELINE_RUNS R
          ON R.RUN_ID = L.PIPELINE_RUN_ID
        WHERE L.STATUS = 'SUCCESS' AND R.STATUS = 'SUCCESS'
        """
    )
    full_run = warehouse.fetch_one(
        f"""
        SELECT BENCHMARK_ID, ROUND(SUM(TOTAL_DURATION_SECONDS), 3)
        FROM {database}.{metadata}.PIPELINE_RUNS
        WHERE STATUS = 'SUCCESS' AND RUN_TYPE = 'FULL' AND BENCHMARK_ID IS NOT NULL
        GROUP BY BENCHMARK_ID
        ORDER BY MAX(COMPLETED_AT) DESC
        LIMIT 1
        """
    )
    incremental_run = warehouse.fetch_one(
        f"""
        SELECT RUN_ID, TOTAL_DURATION_SECONDS
        FROM {database}.{metadata}.PIPELINE_RUNS
        WHERE STATUS = 'SUCCESS' AND RUN_TYPE = 'INCREMENTAL'
        ORDER BY COMPLETED_AT DESC
        LIMIT 1
        """
    )

    full_seconds = _optional_number(full_run, 1)
    incremental_seconds = _optional_number(incremental_run, 1)
    runtime_reduction = None
    if full_seconds and incremental_seconds is not None:
        runtime_reduction = round((full_seconds - incremental_seconds) / full_seconds * 100, 2)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "records_processed": int(warehouse_counts[0]),
        "months_processed": int(warehouse_counts[1]),
        "airlines": int(warehouse_counts[2]),
        "airports": int(warehouse_counts[3]),
        "date_dimension_rows": int(warehouse_counts[4]),
        "raw_file_bytes": int(measured_loads[0]),
        "duplicates_removed": int(measured_loads[1]),
        "invalid_records": int(measured_loads[2]),
        "full_load_seconds": full_seconds,
        "incremental_load_seconds": incremental_seconds,
        "runtime_reduction_percent": runtime_reduction,
        "average_monthly_pipeline_seconds": _optional_number(measured_loads, 3),
        "airflow_task_count": len(AIRFLOW_TASK_IDS),
        "query_performance": warehouse.query_performance(),
    }


def write_report(metrics: dict[str, Any], json_path: Path, markdown_path: Path | None) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(metrics, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if markdown_path:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(_markdown(metrics), encoding="utf-8")


def _optional_number(row: tuple[Any, ...] | None, index: int) -> float | None:
    if not row or row[index] is None:
        return None
    return round(float(row[index]), 3)


def _display(value: Any) -> str:
    if value is None:
        return "Not measured"
    if isinstance(value, float):
        return f"{value:,.3f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def _markdown(metrics: dict[str, Any]) -> str:
    rows = (
        ("Flight records", metrics["records_processed"]),
        ("Months", metrics["months_processed"]),
        ("Airlines", metrics["airlines"]),
        ("Airports", metrics["airports"]),
        ("Raw file bytes", metrics["raw_file_bytes"]),
        ("Duplicates removed", metrics["duplicates_removed"]),
        ("Invalid rows", metrics["invalid_records"]),
        ("Full pipeline runtime (seconds)", metrics["full_load_seconds"]),
        ("Incremental runtime (seconds)", metrics["incremental_load_seconds"]),
        ("Runtime reduction (%)", metrics["runtime_reduction_percent"]),
        ("Average monthly runtime (seconds)", metrics["average_monthly_pipeline_seconds"]),
    )
    table = "\n".join(f"| {name} | {_display(value)} |" for name, value in rows)
    return (
        "# Measured Pipeline Results\n\n"
        f"Generated at `{metrics['generated_at']}` from Snowflake metadata.\n\n"
        "| Metric | Result |\n|---|---:|\n"
        f"{table}\n\n"
        "`Not measured` means the corresponding benchmark has not been executed.\n"
    )

