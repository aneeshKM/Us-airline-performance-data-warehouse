# Warehouse Reference

## Valid flight rule

A raw row enters staging when its flight date belongs to the requested source month; airline,
flight number, origin, destination, and both airport identifiers are present; and distance is
greater than zero and no more than 10,000 miles. Among repeated natural keys, one row is retained.

Cancelled and diverted flights are valid business events and remain in the fact. Their absent
arrival values are not treated as source errors.

## Fact key

`FLIGHT_ID` / `FLIGHT_KEY` is SHA-256 over:

```text
flight date | reporting carrier | flight number | origin | destination | scheduled departure
```

That definition is also the `ROW_NUMBER` partition used for de-duplication.

## Lineage

`SOURCE_MONTH`, `SOURCE_FILE`, `SOURCE_FILE_CHECKSUM`, `SOURCE_RECORD_HASH`, and
`PIPELINE_RUN_ID` connect warehouse rows to their monthly file and measured pipeline run.

## Metadata tables

- `PIPELINE_LOADS`: the latest month-level incremental checkpoint.
- `PIPELINE_RUNS`: run status, row counts, file bytes, and phase timings.
- `DATA_QUALITY_RESULTS`: check-level counts and pass/warn/fail state.

