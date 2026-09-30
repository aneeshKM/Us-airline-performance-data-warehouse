# Airflow and Pipeline Operations

## Manual monthly run

In the Airflow UI, trigger `airline_performance_monthly` with a source month:

```json
{"source_month": "2024-08", "force": false}
```

Use `force: true` only to replace a month whose BTS file was corrected or to exercise
idempotency. The CLI equivalent is:

```bash
airline-warehouse load-month --month 2024-08 --force
```

## Scheduled run

The DAG is scheduled for 06:00 UTC on the fifteenth of each month, permits only one active run,
and starts paused. Its default `AUTO` parameter selects the month two calendar months before the
Airflow interval end. Set a specific `source_month` when manually triggering. Before enabling
unattended scheduling, compare this lag with the current BTS release calendar; publication does
not necessarily align with the calendar month immediately preceding the run.

## Recovery

1. Read the failed Airflow task log.
2. Query the corresponding `METADATA.PIPELINE_RUNS` row for `ERROR_MESSAGE`.
3. Fix the connection, source, or SQL issue.
4. Clear the failed task in Airflow. Completed upstream tasks remain available through XCom and
   task retry; a brand-new DAG run safely replaces only its source month.

Snowflake query history provides the database-side statement details. Because the raw and
staging replacements are month-scoped and the fact uses `MERGE`, replaying a task does not add a
second copy of a flight.

## Cost controls

- Keep `AIRLINE_ETL_WH` at X-Small for the initial run.
- Retain the 60-second auto-suspend setting.
- Run the historical backfill once, then use one-month increments.
- Inspect Snowflake query history before increasing warehouse size.
- Use `2024-01` through `2024-12` first if trial credits are limited.
