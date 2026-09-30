from pathlib import Path
from typing import Any

import pytest

from airline_warehouse.config import Settings
from airline_warehouse.downloader import DownloadResult
from airline_warehouse.month import YearMonth
from airline_warehouse.pipeline import MonthlyPipeline

FIXTURE = Path(__file__).parents[1] / "fixtures" / "flights_2024_01.csv"


class FakeDownloader:
    def download(self, month: YearMonth) -> DownloadResult:
        return DownloadResult(
            source_month=str(month),
            source_url="https://example.test/file.zip",
            archive_path=str(FIXTURE),
            csv_path=str(FIXTURE),
            archive_bytes=100,
            csv_bytes=FIXTURE.stat().st_size,
            checksum_sha256="a" * 64,
            reused_local_file=True,
        )


class FakeWarehouse:
    def __init__(self, loaded: bool = False):
        self.loaded = loaded
        self.calls: list[str] = []
        self.completed: dict[str, Any] | None = None

    def month_is_loaded(self, month: YearMonth) -> bool:
        self.calls.append("check")
        return self.loaded

    def begin_run(self, *args: Any) -> None:
        self.calls.append("begin")

    def upload_file(self, *args: Any) -> str:
        self.calls.append("upload")
        return "2024-01/checksum"

    def load_raw(self, *args: Any) -> None:
        self.calls.append("raw")

    def transform_staging(self, *args: Any) -> None:
        self.calls.append("staging")

    def merge_dimensions_and_fact(self, *args: Any) -> None:
        self.calls.append("model")

    def rebuild_marts(self) -> None:
        self.calls.append("marts")

    def validate_month(self, *args: Any) -> dict[str, int]:
        self.calls.append("quality")
        return {
            "raw_record_count": 4,
            "valid_record_count": 1,
            "duplicate_count": 1,
            "rejected_record_count": 2,
            "fact_record_count": 1,
            "null_route_count": 1,
            "invalid_date_count": 1,
            "invalid_distance_count": 0,
        }

    def complete_run(self, **kwargs: Any) -> None:
        self.calls.append("complete")
        self.completed = kwargs

    def fail_run(self, *args: Any) -> None:
        self.calls.append("fail")


class FailingWarehouse(FakeWarehouse):
    def transform_staging(self, *args: Any) -> None:
        self.calls.append("staging")
        raise RuntimeError("test transformation failure")


def _settings(tmp_path: Path) -> Settings:
    return Settings(account="a", user="u", password="p", raw_data_dir=tmp_path)


def test_loaded_month_is_skipped_before_download(tmp_path: Path) -> None:
    warehouse = FakeWarehouse(loaded=True)
    result = MonthlyPipeline(
        _settings(tmp_path), warehouse=warehouse, downloader=FakeDownloader()
    ).run(YearMonth(2024, 1))
    assert result.status == "SKIPPED"
    assert warehouse.calls == ["check"]


def test_month_pipeline_runs_in_order_and_records_measured_counts(tmp_path: Path) -> None:
    warehouse = FakeWarehouse()
    result = MonthlyPipeline(
        _settings(tmp_path), warehouse=warehouse, downloader=FakeDownloader()
    ).run(YearMonth(2024, 1))
    assert result.status == "SUCCESS"
    assert result.valid_records == 1
    assert warehouse.calls == [
        "check",
        "begin",
        "upload",
        "raw",
        "staging",
        "model",
        "marts",
        "quality",
        "complete",
    ]
    assert warehouse.completed is not None
    assert warehouse.completed["counts"]["duplicate_count"] == 1


def test_failure_is_recorded_after_run_start(tmp_path: Path) -> None:
    warehouse = FailingWarehouse()
    pipeline = MonthlyPipeline(
        _settings(tmp_path), warehouse=warehouse, downloader=FakeDownloader()
    )
    with pytest.raises(RuntimeError, match="test transformation failure"):
        pipeline.run(YearMonth(2024, 1))
    assert warehouse.calls[-1] == "fail"
