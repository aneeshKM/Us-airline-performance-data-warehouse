from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Iterator

from airline_warehouse.config import Settings
from airline_warehouse.month import YearMonth
from airline_warehouse.sql import render_sql, split_sql_statements, template_context

LOGGER = logging.getLogger(__name__)


class SnowflakeWarehouse:
    def __init__(self, settings: Settings):
        self.settings = settings

    @contextmanager
    def connection(self) -> Iterator[Any]:
        try:
            import snowflake.connector
        except ImportError as exc:
            raise RuntimeError(
                "snowflake-connector-python is required; install the project dependencies"
            ) from exc
        connection = snowflake.connector.connect(**self.settings.connection_parameters())
        try:
            yield connection
        finally:
            connection.close()

    def initialize_objects(self) -> None:
        context = template_context(self.settings)
        with self.connection() as connection:
            self._execute_script(connection, "setup/01_objects.sql", context)

    def month_is_loaded(self, month: YearMonth) -> bool:
        sql = f"""
            SELECT COUNT(*)
            FROM {self.settings.database}.{self.settings.metadata_schema}.PIPELINE_LOADS
            WHERE SOURCE_MONTH = %s AND STATUS = 'SUCCESS'
        """
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(sql, (month.first_day,))
            return int(cursor.fetchone()[0]) > 0

    def begin_run(
        self,
        run_id: str,
        month: YearMonth,
        started_at: datetime,
        run_type: str,
        benchmark_id: str | None,
    ) -> None:
        sql = f"""
            INSERT INTO {self.settings.database}.{self.settings.metadata_schema}.PIPELINE_RUNS
              (RUN_ID, SOURCE_MONTH, RUN_TYPE, BENCHMARK_ID, STATUS, STARTED_AT)
            VALUES (%s, %s, %s, %s, 'RUNNING', %s)
        """
        self.execute(sql, (run_id, month.first_day, run_type, benchmark_id, started_at))

    def upload_file(self, path: Path, month: YearMonth, checksum: str) -> str:
        stage_prefix = f"{month}/{checksum}"
        file_uri = path.resolve().as_uri().replace("'", "''")
        sql = (
            f"PUT '{file_uri}' "
            f"@{self.settings.database}.{self.settings.raw_schema}.{self.settings.stage}/"
            f"{stage_prefix} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        self.execute(sql)
        return stage_prefix

    def load_raw(
        self,
        month: YearMonth,
        source_file: str,
        checksum: str,
        run_id: str,
        stage_prefix: str,
    ) -> None:
        context = template_context(
            self.settings,
            source_month=str(month),
            source_file=source_file,
            checksum=checksum,
            run_id=run_id,
            stage_prefix=stage_prefix,
        )
        with self.connection() as connection:
            self._execute_script(connection, "raw/load_month.sql", context)

    def transform_staging(self, month: YearMonth) -> None:
        context = template_context(self.settings, source_month=str(month))
        with self.connection() as connection:
            self._execute_script(connection, "staging/transform_month.sql", context)

    def merge_dimensions_and_fact(self, month: YearMonth) -> None:
        context = template_context(self.settings, source_month=str(month))
        with self.connection() as connection:
            self._execute_script(connection, "dimensions/merge_dimensions.sql", context)
            self._execute_script(connection, "facts/merge_fact_flights.sql", context)

    def rebuild_marts(self) -> None:
        context = template_context(self.settings)
        with self.connection() as connection:
            self._execute_script(connection, "marts/rebuild_marts.sql", context)

    def validate_month(
        self, run_id: str, month: YearMonth, source_record_count: int
    ) -> dict[str, int]:
        counts = self.month_counts(month)
        raw_count = counts["raw_record_count"]
        valid_count = counts["valid_record_count"]
        duplicate_count = counts["duplicate_count"]
        rejected_count = counts["rejected_record_count"]
        fact_count = counts["fact_record_count"]

        checks = [
            (
                "SOURCE_TO_RAW_RECONCILIATION",
                source_record_count,
                abs(source_record_count - raw_count),
                "PASS" if source_record_count == raw_count else "FAIL",
            ),
            (
                "RAW_TO_STAGING_RECONCILIATION",
                raw_count,
                abs(raw_count - valid_count - duplicate_count - rejected_count),
                "PASS"
                if raw_count == valid_count + duplicate_count + rejected_count
                else "FAIL",
            ),
            (
                "STAGING_TO_FACT_RECONCILIATION",
                valid_count,
                abs(valid_count - fact_count),
                "PASS" if valid_count == fact_count else "FAIL",
            ),
            (
                "DUPLICATE_NATURAL_KEYS",
                raw_count,
                duplicate_count,
                "PASS" if duplicate_count == 0 else "WARN",
            ),
            (
                "INVALID_SOURCE_ROWS",
                raw_count,
                rejected_count,
                "PASS" if rejected_count == 0 else "WARN",
            ),
            (
                "NULL_AIRLINE_OR_ROUTE",
                raw_count,
                counts["null_route_count"],
                "PASS" if counts["null_route_count"] == 0 else "WARN",
            ),
            (
                "INVALID_FLIGHT_DATE",
                raw_count,
                counts["invalid_date_count"],
                "PASS" if counts["invalid_date_count"] == 0 else "WARN",
            ),
            (
                "INVALID_DISTANCE",
                raw_count,
                counts["invalid_distance_count"],
                "PASS" if counts["invalid_distance_count"] == 0 else "WARN",
            ),
        ]
        table = f"{self.settings.database}.{self.settings.metadata_schema}.DATA_QUALITY_RESULTS"
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(f"DELETE FROM {table} WHERE RUN_ID = %s", (run_id,))
            cursor.executemany(
                f"""
                INSERT INTO {table}
                  (RUN_ID, SOURCE_MONTH, CHECK_NAME, RECORDS_CHECKED,
                   FAILED_RECORDS, STATUS)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                [
                    (run_id, month.first_day, name, checked, failed, status)
                    for name, checked, failed, status in checks
                ],
            )
        if any(status == "FAIL" for _, _, _, status in checks):
            failed = [name for name, _, _, status in checks if status == "FAIL"]
            raise RuntimeError(f"Warehouse reconciliation failed: {', '.join(failed)}")
        return counts

    def month_counts(self, month: YearMonth) -> dict[str, int]:
        database = self.settings.database
        raw = self.settings.raw_schema
        staging = self.settings.staging_schema
        analytics = self.settings.analytics_schema
        sql = f"""
        WITH R AS (
          SELECT *,
            TRY_TO_DATE(FLIGHTDATE, 'YYYY-MM-DD') AS TYPED_FLIGHT_DATE,
            TRY_TO_DECIMAL(DISTANCE, 10, 2) AS TYPED_DISTANCE,
            TRY_TO_TIME(
              IFF(LPAD(CRSDEPTIME, 4, '0') = '2400',
                  '0000', LPAD(CRSDEPTIME, 4, '0')),
              'HH24MI'
            ) AS TYPED_SCHEDULED_DEPARTURE
          FROM {database}.{raw}.FLIGHTS
          WHERE SOURCE_MONTH = %s
        ), RULES AS (
          SELECT *,
            TYPED_FLIGHT_DATE IS NOT NULL
              AND DATE_TRUNC('MONTH', TYPED_FLIGHT_DATE) = %s AS HAS_VALID_DATE,
            NULLIF(TRIM(REPORTING_AIRLINE), '') IS NOT NULL
              AND NULLIF(TRIM(FLIGHT_NUMBER_REPORTING_AIRLINE), '') IS NOT NULL
              AND TRY_TO_NUMBER(ORIGINAIRPORTID) IS NOT NULL
              AND NULLIF(TRIM(ORIGIN), '') IS NOT NULL
              AND TRY_TO_NUMBER(DESTAIRPORTID) IS NOT NULL
              AND NULLIF(TRIM(DEST), '') IS NOT NULL AS HAS_VALID_ROUTE,
            COALESCE(TYPED_DISTANCE > 0 AND TYPED_DISTANCE <= 10000, FALSE)
              AS HAS_VALID_DISTANCE
          FROM R
        ), CLASSIFIED AS (
          SELECT *,
            HAS_VALID_DATE AND HAS_VALID_ROUTE AND HAS_VALID_DISTANCE AS IS_VALID,
            ROW_NUMBER() OVER (
              PARTITION BY FLIGHTDATE, REPORTING_AIRLINE,
                           FLIGHT_NUMBER_REPORTING_AIRLINE, ORIGIN, DEST,
                           TYPED_SCHEDULED_DEPARTURE
              ORDER BY LOADED_AT DESC, SOURCE_ROW_NUMBER
            ) AS NATURAL_KEY_RANK
          FROM RULES
        )
        SELECT
          (SELECT COUNT(*) FROM R) AS RAW_RECORD_COUNT,
          (SELECT COUNT(*) FROM {database}.{staging}.FLIGHTS_CLEAN WHERE SOURCE_MONTH = %s)
            AS VALID_RECORD_COUNT,
          COUNT_IF(IS_VALID AND NATURAL_KEY_RANK > 1) AS DUPLICATE_COUNT,
          COUNT_IF(NOT IS_VALID) AS REJECTED_RECORD_COUNT,
          (SELECT COUNT(*) FROM {database}.{analytics}.FACT_FLIGHTS WHERE SOURCE_MONTH = %s)
            AS FACT_RECORD_COUNT,
          COUNT_IF(NOT HAS_VALID_ROUTE) AS NULL_ROUTE_COUNT,
          COUNT_IF(NOT HAS_VALID_DATE) AS INVALID_DATE_COUNT,
          COUNT_IF(NOT HAS_VALID_DISTANCE) AS INVALID_DISTANCE_COUNT
        FROM CLASSIFIED
        """
        params = (month.first_day, month.first_day, month.first_day, month.first_day)
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(sql, params)
            row = cursor.fetchone()
        names = (
            "raw_record_count",
            "valid_record_count",
            "duplicate_count",
            "rejected_record_count",
            "fact_record_count",
            "null_route_count",
            "invalid_date_count",
            "invalid_distance_count",
        )
        return {name: int(value or 0) for name, value in zip(names, row, strict=True)}

    def complete_run(
        self,
        *,
        run_id: str,
        month: YearMonth,
        source_file: str,
        checksum: str,
        source_file_bytes: int,
        counts: dict[str, int],
        download_seconds: float,
        load_seconds: float,
        transformation_seconds: float,
        total_seconds: float,
        completed_at: datetime,
    ) -> None:
        database = self.settings.database
        metadata = self.settings.metadata_schema
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute("BEGIN")
            try:
                cursor.execute(
                    f"""
                    UPDATE {database}.{metadata}.PIPELINE_RUNS SET
                      SOURCE_FILE = %s, SOURCE_FILE_BYTES = %s,
                      RAW_RECORD_COUNT = %s, VALID_RECORD_COUNT = %s,
                      DUPLICATE_COUNT = %s, REJECTED_RECORD_COUNT = %s,
                      DOWNLOAD_DURATION_SECONDS = %s, LOAD_DURATION_SECONDS = %s,
                      TRANSFORMATION_DURATION_SECONDS = %s, TOTAL_DURATION_SECONDS = %s,
                      STATUS = 'SUCCESS', COMPLETED_AT = %s
                    WHERE RUN_ID = %s
                    """,
                    (
                        source_file,
                        source_file_bytes,
                        counts["raw_record_count"],
                        counts["valid_record_count"],
                        counts["duplicate_count"],
                        counts["rejected_record_count"],
                        download_seconds,
                        load_seconds,
                        transformation_seconds,
                        total_seconds,
                        completed_at,
                        run_id,
                    ),
                )
                cursor.execute(
                    f"""
                    MERGE INTO {database}.{metadata}.PIPELINE_LOADS TARGET
                    USING (SELECT %s::DATE SOURCE_MONTH, %s SOURCE_FILE,
                                  %s SOURCE_FILE_CHECKSUM, %s RECORDS_LOADED,
                                  %s PIPELINE_RUN_ID) SOURCE
                    ON TARGET.SOURCE_MONTH = SOURCE.SOURCE_MONTH
                    WHEN MATCHED THEN UPDATE SET
                      SOURCE_FILE = SOURCE.SOURCE_FILE,
                      SOURCE_FILE_CHECKSUM = SOURCE.SOURCE_FILE_CHECKSUM,
                      RECORDS_LOADED = SOURCE.RECORDS_LOADED,
                      LOADED_AT = CURRENT_TIMESTAMP(),
                      PIPELINE_RUN_ID = SOURCE.PIPELINE_RUN_ID,
                      STATUS = 'SUCCESS'
                    WHEN NOT MATCHED THEN INSERT
                      (SOURCE_MONTH, SOURCE_FILE, SOURCE_FILE_CHECKSUM,
                       RECORDS_LOADED, PIPELINE_RUN_ID, STATUS)
                    VALUES
                      (SOURCE.SOURCE_MONTH, SOURCE.SOURCE_FILE,
                       SOURCE.SOURCE_FILE_CHECKSUM, SOURCE.RECORDS_LOADED,
                       SOURCE.PIPELINE_RUN_ID, 'SUCCESS')
                    """,
                    (
                        month.first_day,
                        source_file,
                        checksum,
                        counts["fact_record_count"],
                        run_id,
                    ),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def fail_run(self, run_id: str, completed_at: datetime, message: str) -> None:
        table = f"{self.settings.database}.{self.settings.metadata_schema}.PIPELINE_RUNS"
        self.execute(
            f"""
            UPDATE {table}
            SET STATUS = 'FAILED', ERROR_MESSAGE = %s, COMPLETED_AT = %s,
                TOTAL_DURATION_SECONDS = DATEDIFF('MILLISECOND', STARTED_AT, %s) / 1000.0
            WHERE RUN_ID = %s
            """,
            (message[:8000], completed_at, completed_at, run_id),
        )

    def query_performance(self) -> dict[str, float]:
        analytics = f"{self.settings.database}.{self.settings.analytics_schema}"
        queries = {
            "airline_mart_query_seconds": (
                f"SELECT * FROM {analytics}.MART_AIRLINE_PERFORMANCE "
                "ORDER BY TOTAL_FLIGHTS DESC"
            ),
            "route_mart_query_seconds": (
                f"SELECT * FROM {analytics}.MART_ROUTE_PERFORMANCE "
                "ORDER BY TOTAL_FLIGHTS DESC LIMIT 100"
            ),
            "delay_mart_query_seconds": (
                f"SELECT * FROM {analytics}.MART_DELAY_CAUSES ORDER BY YEAR_MONTH, AIRLINE"
            ),
        }
        timings: dict[str, float] = {}
        with self.connection() as connection, connection.cursor() as cursor:
            for name, query in queries.items():
                started = perf_counter()
                cursor.execute(query)
                cursor.fetchall()
                timings[name] = round(perf_counter() - started, 3)
        return timings

    def reset_pipeline_data(self) -> None:
        """Clear project-owned data for an explicitly requested full benchmark."""
        database = self.settings.database
        tables = [
            f"{database}.{self.settings.analytics_schema}.FACT_FLIGHTS",
            f"{database}.{self.settings.analytics_schema}.DIM_DATE",
            f"{database}.{self.settings.analytics_schema}.DIM_AIRPORT",
            f"{database}.{self.settings.analytics_schema}.DIM_AIRLINE",
            f"{database}.{self.settings.staging_schema}.FLIGHTS_CLEAN",
            f"{database}.{self.settings.raw_schema}.FLIGHTS",
            f"{database}.{self.settings.metadata_schema}.DATA_QUALITY_RESULTS",
            f"{database}.{self.settings.metadata_schema}.PIPELINE_LOADS",
            f"{database}.{self.settings.metadata_schema}.PIPELINE_RUNS",
        ]
        with self.connection() as connection, connection.cursor() as cursor:
            for table in tables:
                cursor.execute(f"TRUNCATE TABLE {table}")
            for mart in (
                "MART_AIRLINE_PERFORMANCE",
                "MART_ROUTE_PERFORMANCE",
                "MART_DELAY_CAUSES",
            ):
                cursor.execute(
                    f"DROP TABLE IF EXISTS {database}.{self.settings.analytics_schema}.{mart}"
                )

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(sql, params)

    def fetch_one(self, sql: str, params: tuple[Any, ...] | None = None) -> tuple[Any, ...]:
        with self.connection() as connection, connection.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone()

    def _execute_script(
        self, connection: Any, relative_path: str, context: dict[str, str]
    ) -> None:
        script = render_sql(relative_path, context)
        try:
            with connection.cursor() as cursor:
                for statement in split_sql_statements(script):
                    LOGGER.debug("Executing %s statement", relative_path)
                    cursor.execute(statement)
        except Exception:
            connection.rollback()
            raise
