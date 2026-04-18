"""
NIA provider module.

This project historically used `providers/NID.py` for the NIA integration.
Keep this file as the canonical name going forward.
"""
from __future__ import annotations

# Re-export from the existing implementation to preserve behavior.
from .NID import NIAProvider, ICICIProvider  # noqa: F401

