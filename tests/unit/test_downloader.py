import zipfile
from pathlib import Path

from airline_warehouse.downloader import (
    BtsDownloader,
    _remove_unnamed_trailing_column,
    sha256_file,
)
from airline_warehouse.month import YearMonth


def test_official_reporting_carrier_archive_name() -> None:
    downloader = BtsDownloader("https://example.test/PREZIP", Path("data"))
    month = YearMonth(2024, 1)
    assert (
        downloader.source_url(month)
        == "https://example.test/PREZIP/"
        "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_1.zip"
    )


def test_extracts_largest_non_readme_csv(tmp_path: Path) -> None:
    archive_path = tmp_path / "source.zip"
    output_path = tmp_path / "flights.csv"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("readme.csv", "documentation")
        archive.writestr("small.csv", "x\n1\n")
        archive.writestr("nested/flights.csv", "a,b\n1,2\n3,4\n")
    BtsDownloader("https://example.test", tmp_path)._extract_primary_csv(
        archive_path, output_path
    )
    assert output_path.read_text() == "a,b\n1,2\n3,4\n"
    assert len(sha256_file(output_path)) == 64


def test_removes_bts_empty_trailing_column_atomically(tmp_path: Path) -> None:
    source = tmp_path / "flights.csv"
    source.write_bytes(b'"Year","Origin",\r\n2024,"JFK",\r\n')
    assert _remove_unnamed_trailing_column(source) is True
    assert source.read_bytes() == b'"Year","Origin"\r\n2024,"JFK"\r\n'
    assert _remove_unnamed_trailing_column(source) is False
