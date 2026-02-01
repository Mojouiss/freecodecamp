"""
Unit tests for logger module.
"""

import unittest
import logging
import tempfile
import os

from financial_snapshot.utils.logger import SnapshotLogger


class TestSnapshotLogger(unittest.TestCase):
    """Test SnapshotLogger class."""
    
    def test_initialization_defaults(self):
        """Test logger initialization with defaults."""
        logger = SnapshotLogger("test_logger")
        
        self.assertIsNotNone(logger.logger)
        self.assertEqual(logger.logger.name, "test_logger")
        self.assertEqual(logger.logger.level, logging.INFO)
    
    def test_initialization_custom_level(self):
        """Test logger initialization with custom level."""
        logger = SnapshotLogger("test_logger", level="DEBUG")
        
        self.assertEqual(logger.logger.level, logging.DEBUG)
    
    def test_log_file_creation(self):
        """Test that log file is created."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, "test.log")
            logger = SnapshotLogger(
                "test_logger",
                log_file=log_file,
                enable_console=False,
            )
            
            logger.info("Test message")
            
            self.assertTrue(os.path.exists(log_file))
            with open(log_file, "r") as f:
                content = f.read()
                self.assertIn("Test message", content)
    
    def test_logging_with_context(self):
        """Test logging with context information."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, "test.log")
            logger = SnapshotLogger(
                "test_logger",
                log_file=log_file,
                enable_console=False,
            )
            
            logger.info("Message with context", key1="value1", key2="value2")
            
            with open(log_file, "r") as f:
                content = f.read()
                self.assertIn("Message with context", content)
                self.assertIn("key1=value1", content)
                self.assertIn("key2=value2", content)
    
    def test_log_snapshot_start(self):
        """Test snapshot start logging."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, "test.log")
            logger = SnapshotLogger(
                "test_logger",
                log_file=log_file,
                enable_console=False,
            )
            
            logger.log_snapshot_start("snapshot_123")
            
            with open(log_file, "r") as f:
                content = f.read()
                self.assertIn("Starting snapshot capture", content)
                self.assertIn("snapshot_123", content)
    
    def test_log_snapshot_complete(self):
        """Test snapshot completion logging."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, "test.log")
            logger = SnapshotLogger(
                "test_logger",
                log_file=log_file,
                enable_console=False,
            )
            
            logger.log_snapshot_complete("snapshot_123", 1000, 5.5)
            
            with open(log_file, "r") as f:
                content = f.read()
                self.assertIn("Snapshot capture completed", content)
                self.assertIn("snapshot_123", content)
                self.assertIn("1000", content)


if __name__ == "__main__":
    unittest.main()
