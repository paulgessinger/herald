"""Environment setup that must run before any `herald.*` module is imported.

`herald.config` calls `os.environ[...]` at import time for required vars, so
the test process needs them in place before the first import of the package.
"""

import os
import tempfile
from pathlib import Path

_TMP_ROOT = Path(tempfile.mkdtemp(prefix="herald-test-"))

os.environ.setdefault("HERALD_GH_PRIVATE_KEY", "test-private-key")
os.environ.setdefault("HERALD_GH_APP_ID", "1")
os.environ.setdefault("HERALD_METRICS_SECRET", "test-secret")
os.environ.setdefault("HERALD_CACHE_LOCATION", str(_TMP_ROOT / "files"))
os.environ.setdefault("HERALD_ARTIFACT_CACHE_LOCATION", str(_TMP_ROOT / "artifacts"))
