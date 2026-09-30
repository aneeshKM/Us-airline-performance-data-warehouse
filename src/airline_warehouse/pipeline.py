from __future__ import annotations

import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

from airline_warehouse.config import Settings
from airline_warehouse.downloader import BtsDownloader
from airline_warehouse.month import YearMonth
from airline_warehouse.validation import validate_raw_file
from airline_warehouse.warehouse import SnowflakeWarehouse

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineResult:
    run_id: str | None
    source_month: str
    status: str
    raw_records: int = 0
    valid_records: int = 0
    duplicate_records: int = 0
    rejected_records: int = 0
    total_duration_seconds: float = 0.0

    def as_dict(self) -> dict[str, str | int | float | None]:
        return asdict(self)


class MonthlyPipeline:
    def __init__(
        self,
        settings: Settings,
        *,
        warehouse: SnowflakeWarehouse | None = None,
        downloader: BtsDownloader | None = None,
    ):
        self.settings = settings
        self.warehouse = warehouse or SnowflakeWarehouse(settings)
        self.downloader = downloader or BtsDownloader(
            settings.bts_base_url,
            settings.raw_data_dir,
            settings.request_timeout_seconds,
        )

    def run(
        self,
        month: YearMonth,
        *,
        force: bool = False,
        run_type: str = "INCREMENTAL",
        benchmark_id: str | None = None,
    ) -> PipelineResult:
        if not force and self.warehouse.month_is_loaded(month):
            LOGGER.info("Skipping %s because a successful load already exists", month)
            return PipelineResult(None, str(month), "SKIPPED")

        run_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)
        pipeline_timer = perf_counter()
        self.warehouse.begin_run(run_id, month, started_at, run_type, benchmark_id)
        try:
            timer = perf_counter()
            download = self.downloader.download(month)
            validation = validate_raw_file(Path(download.csv_path), month)
            download_seconds = perf_counter() - timer

            timer = perf_counter()
            stage_prefix = self.warehouse.upload_file(
                Path(download.csv_path), month, download.checksum_sha256
            )
            self.warehouse.load_raw(
                month,
                Path(download.csv_path).name,
                download.checksum_sha256,
                run_id,
                stage_prefix,
            )
            load_seconds = perf_counter() - timer

            timer = perf_counter()
            self.warehouse.transform_staging(month)
            self.warehouse.merge_dimensions_and_fact(month)
            self.warehouse.rebuild_marts()
            transformation_seconds = perf_counter() - timer

            counts = self.warehouse.validate_month(run_id, month, validation.row_count)
            total_seconds = perf_counter() - pipeline_timer
            completed_at = datetime.now(timezone.utc)
            self.warehouse.complete_run(
                run_id=run_id,
                month=month,
                source_file=Path(download.csv_path).name,
                checksum=download.checksum_sha256,
                source_file_bytes=download.csv_bytes,
                counts=counts,
                download_seconds=download_seconds,
                load_seconds=load_seconds,
                transformation_seconds=transformation_seconds,
                total_seconds=total_seconds,
                completed_at=completed_at,
            )
            return PipelineResult(
                run_id,
                str(month),
                "SUCCESS",
                raw_records=counts["raw_record_count"],
                valid_records=counts["valid_record_count"],
                duplicate_records=counts["duplicate_count"],
                rejected_records=counts["rejected_record_count"],
                total_duration_seconds=round(total_seconds, 3),
            )
        except Exception as exc:
            LOGGER.exception("Pipeline failed for %s", month)
            try:
                self.warehouse.fail_run(run_id, datetime.now(timezone.utc), str(exc))
            except Exception:
                LOGGER.exception("Could not persist failure metadata for run %s", run_id)
            raise

