from __future__ import annotations

import hashlib
import logging
import os
import shutil
import tempfile
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from airline_warehouse.month import YearMonth

LOGGER = logging.getLogger(__name__)
ARCHIVE_PREFIX = "On_Time_Reporting_Carrier_On_Time_Performance_1987_present"


@dataclass(frozen=True)
class DownloadResult:
    source_month: str
    source_url: str
    archive_path: str
    csv_path: str
    archive_bytes: int
    csv_bytes: int
    checksum_sha256: str
    reused_local_file: bool

    def as_dict(self) -> dict[str, str | int | bool]:
        return asdict(self)


class BtsDownloader:
    def __init__(self, base_url: str, raw_data_dir: Path, timeout_seconds: int = 120):
        self.base_url = base_url.rstrip("/")
        self.raw_data_dir = Path(raw_data_dir)
        self.timeout_seconds = timeout_seconds

    def archive_name(self, month: YearMonth) -> str:
        return f"{ARCHIVE_PREFIX}_{month.year}_{month.month}.zip"

    def source_url(self, month: YearMonth) -> str:
        return f"{self.base_url}/{self.archive_name(month)}"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=20),
        retry=retry_if_exception_type((requests.RequestException, OSError, zipfile.BadZipFile)),
        reraise=True,
    )
    def download(self, month: YearMonth, *, force: bool = False) -> DownloadResult:
        month_dir = self.raw_data_dir / str(month)
        month_dir.mkdir(parents=True, exist_ok=True)
        archive_path = month_dir / self.archive_name(month)
        csv_path = month_dir / f"flights_{month.year}_{month.month:02d}.csv"

        reused = archive_path.exists() and csv_path.exists() and not force
        if not reused:
            self._download_archive(self.source_url(month), archive_path)
            self._extract_primary_csv(archive_path, csv_path)
        else:
            LOGGER.info("Reusing local BTS files for %s", month)

        if _remove_unnamed_trailing_column(csv_path):
            LOGGER.info("Removed BTS unnamed trailing CSV column for %s", month)
        checksum = sha256_file(csv_path)
        return DownloadResult(
            source_month=str(month),
            source_url=self.source_url(month),
            archive_path=str(archive_path),
            csv_path=str(csv_path),
            archive_bytes=archive_path.stat().st_size,
            csv_bytes=csv_path.stat().st_size,
            checksum_sha256=checksum,
            reused_local_file=reused,
        )

    def _download_archive(self, url: str, destination: Path) -> None:
        LOGGER.info("Downloading %s", url)
        temporary_fd, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.name}.", suffix=".part", dir=destination.parent
        )
        os.close(temporary_fd)
        temporary_path = Path(temporary_name)
        try:
            with requests.get(url, stream=True, timeout=self.timeout_seconds) as response:
                response.raise_for_status()
                with temporary_path.open("wb") as output:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            output.write(chunk)
            if temporary_path.stat().st_size == 0:
                raise ValueError(f"BTS returned an empty archive for {url}")
            temporary_path.replace(destination)
        finally:
            temporary_path.unlink(missing_ok=True)

    def _extract_primary_csv(self, archive_path: Path, destination: Path) -> None:
        temporary_path = destination.with_suffix(".csv.part")
        try:
            with zipfile.ZipFile(archive_path) as archive:
                candidates = [
                    item
                    for item in archive.infolist()
                    if item.filename.lower().endswith(".csv")
                    and "readme" not in item.filename.lower()
                ]
                if not candidates:
                    raise ValueError(f"No flight CSV found in {archive_path}")
                source = max(candidates, key=lambda item: item.file_size)
                with archive.open(source) as compressed, temporary_path.open("wb") as output:
                    shutil.copyfileobj(compressed, output, length=1024 * 1024)
            if temporary_path.stat().st_size == 0:
                raise ValueError(f"Extracted an empty CSV from {archive_path}")
            temporary_path.replace(destination)
        finally:
            temporary_path.unlink(missing_ok=True)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _remove_unnamed_trailing_column(path: Path) -> bool:
    """Remove BTS's empty final CSV field so Snowflake can parse the header by name."""
    with path.open("rb") as source:
        header = source.readline()
    if not _line_body(header).endswith(b","):
        return False

    temporary_path = path.with_suffix(".csv.normalize")
    try:
        with path.open("rb") as source, temporary_path.open("wb") as output:
            for line_number, line in enumerate(source, start=1):
                body, ending = _split_line_ending(line)
                if not body.endswith(b","):
                    raise ValueError(
                        f"Expected an empty trailing BTS field on line {line_number} of {path}"
                    )
                output.write(body[:-1] + ending)
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)
    return True


def _line_body(line: bytes) -> bytes:
    return _split_line_ending(line)[0]


def _split_line_ending(line: bytes) -> tuple[bytes, bytes]:
    if line.endswith(b"\r\n"):
        return line[:-2], b"\r\n"
    if line.endswith(b"\n"):
        return line[:-1], b"\n"
    return line, b""
