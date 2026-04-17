"""Pytest conftest — re-exports fixtures from e2e_helpers."""

from __future__ import annotations

import sys
import os

# Make the tests directory importable
sys.path.insert(0, os.path.dirname(__file__))
