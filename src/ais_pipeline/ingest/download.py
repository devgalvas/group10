"""Download DMA daily AIS zips into the landing zone."""

from __future__ import annotations

import hashlib
import logging
import time
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import requests

from ais_pipeline.config import Settings
from ais_pipeline.paths import landing_path

logger = logging.getLogger(__name__)

CHUNK_BYTES = 8 * 1024 * 1024
MAX_ATTEMPTS = 3
TIMEOUT_S = (10, 120)  # connect, read


class DownloadError(RuntimeError):
    """Raised when a file cannot be downloaded as a valid zip after all retries."""


@dataclass(frozen=True)
class DownloadResult:
    path: Path
    sha256: str
    file_bytes: int
    skipped: bool
    seconds: float


def file_name(settings: Settings, day: date) -> str:
    return settings.source.file_pattern.format(date=day.isoformat())


def source_url(settings: Settings, day: date) -> str:
    return settings.source.base_url.rstrip("/") + "/" + file_name(settings, day)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fetch(session: requests.Session, url: str, part: Path) -> str:
    """Stream url into part, returning its SHA-256. Raises on HTTP errors or a corrupt zip."""
    digest = hashlib.sha256()
    with session.get(url, stream=True, timeout=TIMEOUT_S) as resp:
        resp.raise_for_status()
        with part.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=CHUNK_BYTES):
                fh.write(chunk)
                digest.update(chunk)
    if not zipfile.is_zipfile(part):
        raise DownloadError(f"{url} did not return a valid zip")
    return digest.hexdigest()


def download_day(
    settings: Settings,
    day: date,
    session: requests.Session | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> DownloadResult:
    """Fetch one day's zip to landing/date=YYYY-MM-DD/, skipping it if a valid zip is already there.

    The file is streamed to `<name>.part` and renamed only once it is a complete, valid zip,
    so a crash never leaves a file that looks finished. Failed attempts are retried with
    exponential backoff.
    """
    target_dir = landing_path(settings, day)
    target = target_dir / file_name(settings, day)
    started = time.monotonic()

    if target.is_file() and zipfile.is_zipfile(target):
        logger.info("skip download, valid zip present: %s", target)
        return DownloadResult(target, sha256_file(target), target.stat().st_size, True, 0.0)

    target_dir.mkdir(parents=True, exist_ok=True)
    part = target.with_name(target.name + ".part")
    url = source_url(settings, day)
    session = session or requests.Session()

    try:
        return _download_with_retries(session, url, part, target, started, sleep)
    except DownloadError:
        # Do not leave an empty date=... folder behind for a day that was never fetched.
        if target_dir.is_dir() and not any(target_dir.iterdir()):
            target_dir.rmdir()
        raise


def _download_with_retries(
    session: requests.Session,
    url: str,
    part: Path,
    target: Path,
    started: float,
    sleep: Callable[[float], None],
) -> DownloadResult:
    """Fetch url into target via part, retrying with exponential backoff (404 is not retried)."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            logger.info("downloading %s (attempt %d/%d)", url, attempt, MAX_ATTEMPTS)
            sha = _fetch(session, url, part)
            part.replace(target)
            seconds = time.monotonic() - started
            size = target.stat().st_size
            logger.info("downloaded %s: %d bytes in %.1f s", target.name, size, seconds)
            return DownloadResult(target, sha, size, False, seconds)
        except (requests.RequestException, DownloadError) as exc:
            part.unlink(missing_ok=True)
            if isinstance(exc, requests.HTTPError) and exc.response is not None:
                if exc.response.status_code == 404:
                    raise DownloadError(f"{url} not found (not published yet?)") from exc
            if attempt == MAX_ATTEMPTS:
                raise DownloadError(f"{url} failed after {MAX_ATTEMPTS} attempts: {exc}") from exc
            backoff = 2 ** (attempt - 1)
            logger.warning("download failed (%s), retrying in %d s", exc, backoff)
            sleep(backoff)
    raise AssertionError("unreachable")
