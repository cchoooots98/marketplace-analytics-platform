"""Repository-root pytest configuration.

Pytest does not automatically add the repository root to ``sys.path`` when it
is invoked as the bare ``pytest`` command. This conftest ensures that running
``pytest`` and ``python -m pytest`` resolve the same import paths so that
``ingestion``, ``dashboards``, and ``tasks`` are importable from the test
suite without requiring an editable install.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
