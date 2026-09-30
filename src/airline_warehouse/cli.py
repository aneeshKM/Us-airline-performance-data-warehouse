from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from pathlib import Path

from airline_warehouse.config import Settings
from airline_warehouse.downloader import BtsDownloader
from airline_warehouse.month import YearMonth, month_range
from airline_warehouse.pipeline import MonthlyPipeline
from airline_warehouse.reporting import collect_metrics, write_report
from airline_warehouse.validation import validate_raw_file
from airline_warehouse.warehouse import SnowflakeWarehouse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="US Airline Performance warehouse CLI")
    parser.add_argument("--verbose", action="store_true")
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("init", help="Create Snowflake schemas, tables, stage, and metadata")

    download = commands.add_parser("download", help="Download and validate one BTS month")
    download.add_argument("--month", required=True, type=YearMonth.parse)
    download.add_argument("--force", action="store_true")

    load = commands.add_parser("load-month", help="Run one month end to end")
    load.add_argument("--month", required=True, type=YearMonth.parse)
    load.add_argument("--force", action="store_true")
    load.add_argument(
        "--run-type",
        choices=("INCREMENTAL", "FULL", "IDEMPOTENCY"),
        default="INCREMENTAL",
    )

    backfill = commands.add_parser("backfill", help="Process an inclusive month range")
    backfill.add_argument("--start", required=True, type=YearMonth.parse)
    backfill.add_argument("--end", required=True, type=YearMonth.parse)
    backfill.add_argument("--force", action="store_true")
    backfill.add_argument("--run-type", choices=("INCREMENTAL", "FULL"), default="INCREMENTAL")

    benchmark = commands.add_parser(
        "benchmark", help="Reset project tables, time a full range, then ingest one new month"
    )
    benchmark.add_argument("--start", required=True, type=YearMonth.parse)
    benchmark.add_argument("--end", required=True, type=YearMonth.parse)
    benchmark.add_argument("--incremental-month", required=True, type=YearMonth.parse)
    benchmark.add_argument(
        "--confirm-reset",
        action="store_true",
        help="Required because this clears every project-owned warehouse table",
    )

    idem = commands.add_parser("idempotency-test", help="Force the same month twice")
    idem.add_argument("--month", required=True, type=YearMonth.parse)
    idem.add_argument("--output", type=Path, default=Path("reports/idempotency.json"))

    report = commands.add_parser("report", help="Generate reports from measured Snowflake data")
    report.add_argument("--output", type=Path, default=Path("reports/pipeline_metrics.json"))
    report.add_argument("--markdown", type=Path, default=Path("reports/benchmark.md"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        require_credentials = args.command != "download"
        settings = Settings.from_env(require_credentials=require_credentials)
        if args.command == "download":
            downloader = BtsDownloader(
                settings.bts_base_url, settings.raw_data_dir, settings.request_timeout_seconds
            )
            result = downloader.download(args.month, force=args.force)
            validation = validate_raw_file(Path(result.csv_path), args.month)
            print(json.dumps({**result.as_dict(), "validation": validation.as_dict()}, indent=2))
            return 0

        warehouse = SnowflakeWarehouse(settings)
        if args.command == "init":
            warehouse.initialize_objects()
            print("Snowflake project objects initialized.")
        elif args.command == "load-month":
            result = MonthlyPipeline(settings, warehouse=warehouse).run(
                args.month, force=args.force, run_type=args.run_type
            )
            print(json.dumps(result.as_dict(), indent=2))
        elif args.command == "backfill":
            benchmark_id = str(uuid.uuid4()) if args.run_type == "FULL" else None
            pipeline = MonthlyPipeline(settings, warehouse=warehouse)
            for month in month_range(args.start, args.end):
                result = pipeline.run(
                    month,
                    force=args.force,
                    run_type=args.run_type,
                    benchmark_id=benchmark_id,
                )
                print(json.dumps(result.as_dict()))
        elif args.command == "benchmark":
            if not args.confirm_reset:
                raise ValueError("benchmark requires --confirm-reset")
            if args.incremental_month <= args.end:
                raise ValueError("--incremental-month must be after the full-build range")
            warehouse.reset_pipeline_data()
            benchmark_id = str(uuid.uuid4())
            pipeline = MonthlyPipeline(settings, warehouse=warehouse)
            for month in month_range(args.start, args.end):
                print(
                    json.dumps(
                        pipeline.run(
                            month,
                            force=True,
                            run_type="FULL",
                            benchmark_id=benchmark_id,
                        ).as_dict()
                    )
                )
            print(json.dumps(pipeline.run(args.incremental_month).as_dict()))
            metrics = collect_metrics(warehouse)
            write_report(
                metrics, Path("reports/pipeline_metrics.json"), Path("reports/benchmark.md")
            )
        elif args.command == "idempotency-test":
            pipeline = MonthlyPipeline(settings, warehouse=warehouse)
            first = pipeline.run(args.month, force=True, run_type="IDEMPOTENCY")
            first_count = warehouse.month_counts(args.month)["fact_record_count"]
            second = pipeline.run(args.month, force=True, run_type="IDEMPOTENCY")
            second_count = warehouse.month_counts(args.month)["fact_record_count"]
            result = {
                "source_month": str(args.month),
                "first_run_id": first.run_id,
                "second_run_id": second.run_id,
                "fact_rows_after_first_run": first_count,
                "fact_rows_after_second_run": second_count,
                "new_duplicates": second_count - first_count,
                "passed": first_count == second_count,
            }
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
            print(json.dumps(result, indent=2))
            if not result["passed"]:
                return 1
        elif args.command == "report":
            metrics = collect_metrics(warehouse)
            write_report(metrics, args.output, args.markdown)
            print(json.dumps(metrics, indent=2))
        return 0
    except Exception as exc:
        logging.getLogger(__name__).error("%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
