"""
Unit tests for logger module.
"""

import unittest
import logging
import tempfile
import os
from io import StringIO

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
    
    def test_debug_logging(self):
        """Test debug level logging."""
        logger = SnapshotLogger("test_logger", level="DEBUG", enable_console=False)
        
        # Should not raise any exception
        logger.debug("Debug message")
    
    def test_info_logging(self):
        """Test info level logging."""
        logger = SnapshotLogger("test_logger", enable_console=False)
        
        logger.info("Info message")
    
    def test_warning_logging(self):
        """Test warning level logging."""
        logger = SnapshotLogger("test_logger", enable_console=False)
        
        logger.warning("Warning message")
    
    def test_error_logging(self):
        """Test error level logging."""
        logger = SnapshotLogger("test_logger", enable_console=False)
        
        logger.error("Error message")
    
    def test_critical_logging(self):
        """Test critical level logging."""
        logger = SnapshotLogger("test_logger", enable_console=False)
        
        logger.critical("Critical message")
    
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
                self.assertIn("5.50", content)
    
    def test_log_kafka_consumption(self):
        """Test Kafka consumption logging."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, "test.log")
            logger = SnapshotLogger(
                "test_logger",
                level="DEBUG",
                log_file=log_file,
                enable_console=False,
            )
            
            logger.log_kafka_consumption("test_topic", 0, 12345, 100)
            
            with open(log_file, "r") as f:
                content = f.read()
                self.assertIn("test_topic", content)
                self.assertIn("100", content)
    
    def test_log_database_write(self):
        """Test database write logging."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, "test.log")
            logger = SnapshotLogger(
                "test_logger",
                log_file=log_file,
                enable_console=False,
            )
            
            logger.log_database_write("positions", 500, 2.5)
            
            with open(log_file, "r") as f:
                content = f.read()
                self.assertIn("Written records to database", content)
                self.assertIn("positions", content)
                self.assertIn("500", content)
    
    def test_log_error_with_retry(self):
        """Test error with retry logging."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, "test.log")
            logger = SnapshotLogger(
                "test_logger",
                log_file=log_file,
                enable_console=False,
            )
            
            error = ValueError("Test error")
            logger.log_error_with_retry("test_operation", error, 2, 3)
            
            with open(log_file, "r") as f:
                content = f.read()
                self.assertIn("test_operation", content)
                self.assertIn("Test error", content)
                self.assertIn("ValueError", content)


if __name__ == "__main__":
    unittest.main()
