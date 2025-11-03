"""Pytest configuration ensuring the project root is importable.

The repository relies on implicit imports such as ``import context`` or
``import pipelines``.  When pytest collects tests from a temporary working
directory the repository root is not guaranteed to appear on ``sys.path``.
Adding this ``conftest`` module forces the project root onto the import path so
tests can import the in-repo packages without additional configuration.
"""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

