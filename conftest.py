"""Pytest defaults for the local BoatRace predictor."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


_TEMP_ROOT = Path(tempfile.gettempdir()) / "boatracedb-test-runtime"
_JOBLIB_TEMP = _TEMP_ROOT / "joblib"

_JOBLIB_TEMP.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("JOBLIB_TEMP_FOLDER", str(_JOBLIB_TEMP))
