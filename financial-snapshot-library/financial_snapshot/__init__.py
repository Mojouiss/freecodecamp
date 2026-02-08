"""
Financial Snapshot Library

A robust, high-performance library for capturing snapshots of live financial
positions from Kafka and storing them in SQL databases with optimal performance
and reliability.
"""

__version__ = "1.0.0"
__author__ = "freeCodeCamp"

from .core.snapshot_manager import SnapshotManager
from .core.config import Config

__all__ = ["SnapshotManager", "Config"]
