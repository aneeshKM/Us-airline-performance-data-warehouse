from __future__ import annotations

import csv
import hashlib
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

from airline_warehouse.month import YearMonth

REQUIRED_COLUMNS = {
    "Year",
    "Month",
    "DayofMonth",
    "FlightDate",
    "Reporting_Airline",
    "Flight_Number_Reporting_Airline",
    "OriginAirportID",
    "Origin",
    "DestAirportID",
    "Dest",
    "CRSDepTime",
    "CRSArrTime",
    "Cancelled",
    "Diverted",
    "Distance",
}


class RawFileValidationError(ValueError):
    """Raised when a file cannot safely enter the warehouse."""


@dataclass(frozen=True)
class ValidationResult:
    source_month: str
    row_count: int
    column_count: int
    duplicate_natural_keys: int
    invalid_rows: int
    null_route_rows: int
    invalid_date_rows: int
    invalid_distance_rows: int

    def as_dict(self) -> dict[str, str | int]:
        return asdict(self)


def validate_raw_file(path: Path, expected_month: YearMonth) -> ValidationResult:
    path = Path(path)
    if not path.is_file():
        raise RawFileValidationError(f"Source file does not exist: {path}")
    if path.stat().st_size == 0:
        raise RawFileValidationError(f"Source file is empty: {path}")

    row_count = 0
    duplicate_count = 0
    invalid_rows = 0
    null_route_rows = 0
    invalid_date_rows = 0
    invalid_distance_rows = 0
    seen_keys: set[bytes] = set()

    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        headers = set(reader.fieldnames or [])
        missing = sorted(REQUIRED_COLUMNS - headers)
        if missing:
            raise RawFileValidationError(f"Missing required BTS columns: {', '.join(missing)}")

        for row in reader:
            row_count += 1
            route_invalid = any(
                not (row.get(column) or "").strip()
                for column in ("Reporting_Airline", "Origin", "Dest")
            )
            date_invalid = not _valid_date(row, expected_month)
            distance_invalid = not _valid_distance(row.get("Distance", ""))
            if route_invalid:
                null_route_rows += 1
            if date_invalid:
                invalid_date_rows += 1
            if distance_invalid:
                invalid_distance_rows += 1
            if route_invalid or date_invalid or distance_invalid:
                invalid_rows += 1

            natural_key = "|".join(
                (row.get(column) or "").strip()
                for column in (
                    "FlightDate",
                    "Reporting_Airline",
                    "Flight_Number_Reporting_Airline",
                    "Origin",
                    "Dest",
                    "CRSDepTime",
                )
            )
            key_digest = hashlib.blake2b(natural_key.encode(), digest_size=16).digest()
            if key_digest in seen_keys:
                duplicate_count += 1
            else:
                seen_keys.add(key_digest)

    if row_count == 0:
        raise RawFileValidationError(f"Source file contains no data rows: {path}")

    return ValidationResult(
        source_month=str(expected_month),
        row_count=row_count,
        column_count=len(headers),
        duplicate_natural_keys=duplicate_count,
        invalid_rows=invalid_rows,
        null_route_rows=null_route_rows,
        invalid_date_rows=invalid_date_rows,
        invalid_distance_rows=invalid_distance_rows,
    )


def _valid_date(row: dict[str, str], expected_month: YearMonth) -> bool:
    try:
        parsed = date.fromisoformat((row.get("FlightDate") or "").strip())
        declared_year = int(row.get("Year") or "")
        declared_month = int(row.get("Month") or "")
    except (TypeError, ValueError):
        return False
    return (
        parsed.year == expected_month.year
        and parsed.month == expected_month.month
        and declared_year == parsed.year
        and declared_month == parsed.month
    )


def _valid_distance(value: str) -> bool:
    try:
        distance = float(value)
    except (TypeError, ValueError):
        return False
    return 0 < distance <= 10_000

