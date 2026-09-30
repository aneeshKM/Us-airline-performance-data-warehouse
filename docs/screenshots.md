# README Screenshot Checklist

Capture these only after a successful run so every image reflects real state:

1. Airflow graph view showing all ten tasks green for one incremental month.
2. Airflow task log for `check_month` on a previously loaded month showing the safe skip.
3. Snowflake object browser with the `RAW`, `STAGING`, `ANALYTICS`, and `METADATA` schemas.
4. Snowflake query result for `SELECT COUNT(*) FROM ANALYTICS.FACT_FLIGHTS`.
5. `MART_AIRLINE_PERFORMANCE` ordered by on-time percentage.
6. `MART_ROUTE_PERFORMANCE` ordered by total flights.
7. `METADATA.DATA_QUALITY_RESULTS` for a successful run.
8. The generated `reports/benchmark.md` table.
9. The idempotency JSON showing equal first-run and second-run fact counts.

Hide account identifiers, usernames, browser profile details, query IDs, and any credentials
before publishing screenshots.

