"""Pipeline run identifiers (extended with the pipeline_runs logger in AIS-37)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime


def new_run_id() -> str:
    """Return a sortable, unique run id such as 20261023T141500Z-1a2b3c4d."""
    return f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
