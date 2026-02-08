"""
Logging utilities for the financial snapshot library.

Provides structured logging with context and performance tracking.
"""

import logging
import sys
from typing import Optional
from pathlib import Path
from datetime import datetime


class SnapshotLogger:
    """
    Custom logger for financial snapshot operations.
    
    Provides structured logging with context and metrics.
    """
    
    def __init__(
        self,
        name: str,
        level: str = "INFO",
        log_format: Optional[str] = None,
        log_file: Optional[str] = None,
        enable_console: bool = True,
    ):
        """
        Initialize the logger.
        
        Args:
            name: Logger name
            level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_format: Custom log format string
            log_file: Path to log file (optional)
            enable_console: Whether to log to console
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Set format
        if log_format is None:
            log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        formatter = logging.Formatter(log_format)
        
        # Console handler
        if enable_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        
        # File handler
        if log_file:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
    
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message with optional context."""
        self._log_with_context(logging.DEBUG, message, kwargs)
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message with optional context."""
        self._log_with_context(logging.INFO, message, kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message with optional context."""
        self._log_with_context(logging.WARNING, message, kwargs)
    
    def error(self, message: str, **kwargs) -> None:
        """Log error message with optional context."""
        self._log_with_context(logging.ERROR, message, kwargs)
    
    def critical(self, message: str, **kwargs) -> None:
        """Log critical message with optional context."""
        self._log_with_context(logging.CRITICAL, message, kwargs)
    
    def _log_with_context(self, level: int, message: str, context: dict) -> None:
        """
        Log message with context information.
        
        Args:
            level: Logging level
            message: Log message
            context: Additional context as key-value pairs
        """
        if context:
            context_str = " | ".join(f"{k}={v}" for k, v in context.items())
            message = f"{message} | {context_str}"
        
        self.logger.log(level, message)
    
    def log_snapshot_start(self, snapshot_id: str) -> None:
        """Log the start of a snapshot operation."""
        self.info(
            "Starting snapshot capture",
            snapshot_id=snapshot_id,
            timestamp=datetime.utcnow().isoformat(),
        )
    
    def log_snapshot_complete(
        self, snapshot_id: str, record_count: int, duration_seconds: float
    ) -> None:
        """Log the completion of a snapshot operation."""
        self.info(
            "Snapshot capture completed",
            snapshot_id=snapshot_id,
            record_count=record_count,
            duration_seconds=f"{duration_seconds:.2f}",
            records_per_second=f"{record_count / duration_seconds:.2f}" if duration_seconds > 0 else "N/A",
        )
    
    def log_kafka_consumption(
        self, topic: str, partition: int, offset: int, record_count: int
    ) -> None:
        """Log Kafka consumption metrics."""
        self.debug(
            "Consumed records from Kafka",
            topic=topic,
            partition=partition,
            offset=offset,
            record_count=record_count,
        )
    
    def log_database_write(
        self, table: str, record_count: int, duration_seconds: float
    ) -> None:
        """Log database write operation."""
        self.info(
            "Written records to database",
            table=table,
            record_count=record_count,
            duration_seconds=f"{duration_seconds:.2f}",
            records_per_second=f"{record_count / duration_seconds:.2f}" if duration_seconds > 0 else "N/A",
        )
    
    def log_error_with_retry(
        self, operation: str, error: Exception, retry_count: int, max_retries: int
    ) -> None:
        """Log error with retry information."""
        self.warning(
            f"Error in {operation}, retrying",
            error=str(error),
            error_type=type(error).__name__,
            retry_count=retry_count,
            max_retries=max_retries,
        )
