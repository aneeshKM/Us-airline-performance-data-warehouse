from pathlib import Path

import pytest

from airline_warehouse.month import YearMonth
from airline_warehouse.validation import RawFileValidationError, validate_raw_file

FIXTURE = Path(__file__).parents[1] / "fixtures" / "flights_2024_01.csv"


def test_streaming_validation_measures_quality_issues() -> None:
    result = validate_raw_file(FIXTURE, YearMonth(2024, 1))
    assert result.row_count == 4
    assert result.duplicate_natural_keys == 1
    assert result.invalid_rows == 2
    assert result.null_route_rows == 1
    assert result.invalid_date_rows == 1
    assert result.invalid_distance_rows == 0


def test_missing_columns_fail_fast(tmp_path: Path) -> None:
    source = tmp_path / "bad.csv"
    source.write_text("Year,Month\n2024,1\n", encoding="utf-8")
    with pytest.raises(RawFileValidationError, match="Missing required BTS columns"):
        validate_raw_file(source, YearMonth(2024, 1))

