import io
import zipfile
from datetime import date

import pytest
import requests

from ais_pipeline.config import load_settings
from ais_pipeline.ingest.download import DownloadError, download_day, sha256_file, source_url

DAY = date(2026, 9, 1)


def _zip_bytes() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("aisdk-2026-09-01.csv", "# Timestamp,MMSI\n01/09/2026 00:00:00,219000000\n")
    return buf.getvalue()


class FakeResponse:
    def __init__(self, status: int = 200, body: bytes = b""):
        self.status_code = status
        self.body = body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    def iter_content(self, chunk_size):
        for i in range(0, len(self.body), chunk_size):
            yield self.body[i : i + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeSession:
    """Returns the queued responses in order; an Exception in the queue is raised instead."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.urls = []

    def get(self, url, **kwargs):
        self.urls.append(url)
        nxt = self.responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


@pytest.fixture
def settings(monkeypatch, tmp_path):
    monkeypatch.setenv("AIS_DATA_ROOT", str(tmp_path))
    return load_settings("ci")


def test_source_url(settings):
    assert source_url(settings, DAY) == "http://aisdata.ais.dk/aisdk-2026-09-01.zip"


def test_success_writes_zip_atomically(settings, tmp_path):
    body = _zip_bytes()
    session = FakeSession(FakeResponse(body=body))
    result = download_day(settings, DAY, session=session, sleep=lambda s: None)

    assert result.path == tmp_path / "landing" / "date=2026-09-01" / "aisdk-2026-09-01.zip"
    assert result.path.read_bytes() == body
    assert result.sha256 == sha256_file(result.path)
    assert not result.skipped
    assert not list(result.path.parent.glob("*.part"))


def test_retry_then_success(settings):
    sleeps = []
    session = FakeSession(
        requests.ConnectionError("reset"), FakeResponse(500), FakeResponse(body=_zip_bytes())
    )
    result = download_day(settings, DAY, session=session, sleep=sleeps.append)
    assert result.path.is_file()
    assert sleeps == [1, 2]
    assert len(session.urls) == 3


def test_corrupt_zip_fails_after_retries(settings):
    session = FakeSession(*[FakeResponse(body=b"<html>not a zip</html>")] * 3)
    with pytest.raises(DownloadError, match="after 3 attempts"):
        download_day(settings, DAY, session=session, sleep=lambda s: None)
    target_dir = settings.landing_dir / "date=2026-09-01"
    assert not any(target_dir.iterdir())


def test_404_fails_without_retry(settings):
    session = FakeSession(FakeResponse(404))
    with pytest.raises(DownloadError, match="not found"):
        download_day(settings, DAY, session=session, sleep=lambda s: None)
    assert len(session.urls) == 1


def test_existing_valid_zip_is_skipped(settings):
    download_day(settings, DAY, session=FakeSession(FakeResponse(body=_zip_bytes())))
    session = FakeSession()
    result = download_day(settings, DAY, session=session)
    assert result.skipped
    assert session.urls == []


def test_existing_corrupt_file_is_downloaded_again(settings):
    target_dir = settings.landing_dir / "date=2026-09-01"
    target_dir.mkdir(parents=True)
    (target_dir / "aisdk-2026-09-01.zip").write_bytes(b"truncated")
    result = download_day(settings, DAY, session=FakeSession(FakeResponse(body=_zip_bytes())))
    assert not result.skipped
    assert zipfile.is_zipfile(result.path)
