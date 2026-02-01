"""
Main snapshot manager orchestrating the entire snapshot process.

Coordinates Kafka consumption, data processing, and SQL storage with
scheduling, error handling, and monitoring.
"""

from typing import Optional, Dict, Any
import time
from datetime import datetime
import uuid
from threading import Event, Thread

from .config import Config
from ..messaging.kafka_consumer import FinancialPositionConsumer
from ..storage.sql_writer import FinancialPositionWriter
from ..utils.logger import SnapshotLogger
from ..utils.data_utils import Deduplicator, DataValidator


class SnapshotManager:
    """
    Main orchestrator for financial snapshot operations.
    
    Manages the complete lifecycle of snapshot capture from Kafka to SQL,
    including scheduling, error handling, and performance monitoring.
    """
    
    def __init__(self, config: Config):
        """
        Initialize the snapshot manager.
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.config.validate()
        
        # Initialize logger
        self.logger = SnapshotLogger(
            name="SnapshotManager",
            level=self.config.logging.level,
            log_format=self.config.logging.format,
            log_file=self.config.logging.log_file,
            enable_console=self.config.logging.enable_console,
        )
        
        # Initialize components
        self.consumer = FinancialPositionConsumer(
            config=self.config.kafka,
            logger=self.logger,
        )
        self.writer = FinancialPositionWriter(
            config=self.config.database,
            logger=self.logger,
        )
        
        # Initialize utilities
        self.deduplicator = None
        if self.config.snapshot.enable_deduplication:
            self.deduplicator = Deduplicator(
                key_fields=self.config.snapshot.deduplication_keys,
            )
        
        # Scheduling
        self._stop_event = Event()
        self._scheduler_thread: Optional[Thread] = None
        
        # Metrics
        self.metrics: Dict[str, Any] = {
            "total_snapshots": 0,
            "total_records_processed": 0,
            "total_records_written": 0,
            "total_invalid_records": 0,
            "total_errors": 0,
            "last_snapshot_time": None,
            "last_snapshot_duration": None,
        }
    
    def start(self) -> None:
        """
        Start the snapshot manager with scheduled execution.
        
        Runs snapshot capture every configured interval.
        """
        self.logger.info(
            "Starting snapshot manager",
            interval_seconds=self.config.snapshot.interval_seconds,
        )
        
        # Connect to Kafka and database
        self.consumer.connect()
        self.writer.connect()
        
        # Start scheduler thread
        self._stop_event.clear()
        self._scheduler_thread = Thread(target=self._run_scheduler, daemon=True)
        self._scheduler_thread.start()
        
        self.logger.info("Snapshot manager started successfully")
    
    def stop(self) -> None:
        """Stop the snapshot manager."""
        self.logger.info("Stopping snapshot manager")
        
        self._stop_event.set()
        
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=10)
        
        # Disconnect
        self.consumer.disconnect()
        self.writer.disconnect()
        
        self.logger.info("Snapshot manager stopped")
    
    def _run_scheduler(self) -> None:
        """
        Internal scheduler loop.
        
        Executes snapshots at configured intervals.
        """
        while not self._stop_event.is_set():
            try:
                # Execute snapshot
                self.capture_snapshot()
                
                # Wait for next interval
                self._stop_event.wait(self.config.snapshot.interval_seconds)
                
            except Exception as e:
                self.logger.error(
                    "Unexpected error in scheduler",
                    error=str(e),
                    error_type=type(e).__name__,
                )
                self.metrics["total_errors"] += 1
                
                # Wait before retrying
                time.sleep(self.config.snapshot.retry_backoff_seconds)
    
    def capture_snapshot(self) -> Dict[str, Any]:
        """
        Capture a single snapshot.
        
        Returns:
            Dictionary containing snapshot metrics
        """
        snapshot_id = self._generate_snapshot_id()
        start_time = time.time()
        
        self.logger.log_snapshot_start(snapshot_id)
        
        result = {
            "snapshot_id": snapshot_id,
            "success": False,
            "records_consumed": 0,
            "records_valid": 0,
            "records_written": 0,
            "records_invalid": 0,
            "duration_seconds": 0,
            "error": None,
        }
        
        retry_count = 0
        max_retries = self.config.snapshot.max_retries
        
        while retry_count <= max_retries:
            try:
                # Consume records from Kafka
                records = self.consumer.consume_batch(
                    timeout_ms=self.config.snapshot.processing_timeout_seconds * 1000,
                )
                result["records_consumed"] = len(records)
                
                if not records:
                    self.logger.info("No records consumed, skipping snapshot")
                    result["success"] = True
                    break
                
                # Validate records
                valid_records, invalid_records = DataValidator.validate_batch(records)
                result["records_valid"] = len(valid_records)
                result["records_invalid"] = len(invalid_records)
                
                if invalid_records:
                    self.logger.warning(
                        "Invalid records found",
                        count=len(invalid_records),
                    )
                    for record, error in invalid_records[:5]:  # Log first 5
                        self.logger.debug("Invalid record", error=error)
                
                # Deduplicate if enabled
                if self.deduplicator and valid_records:
                    original_count = len(valid_records)
                    valid_records = self.deduplicator.deduplicate(valid_records)
                    dedup_count = original_count - len(valid_records)
                    
                    if dedup_count > 0:
                        self.logger.info(
                            "Deduplicated records",
                            removed=dedup_count,
                            remaining=len(valid_records),
                        )
                
                # Write to database
                if valid_records:
                    records_written = self.writer.write_batch(
                        records=valid_records,
                        snapshot_id=snapshot_id,
                    )
                    result["records_written"] = records_written
                
                # Commit Kafka offsets
                self.consumer.commit()
                
                result["success"] = True
                break
                
            except Exception as e:
                retry_count += 1
                result["error"] = str(e)
                
                if retry_count <= max_retries:
                    self.logger.log_error_with_retry(
                        operation="capture_snapshot",
                        error=e,
                        retry_count=retry_count,
                        max_retries=max_retries,
                    )
                    time.sleep(self.config.snapshot.retry_backoff_seconds)
                else:
                    self.logger.error(
                        "Max retries exceeded for snapshot",
                        snapshot_id=snapshot_id,
                        error=str(e),
                    )
                    self.metrics["total_errors"] += 1
        
        # Calculate duration and update metrics
        duration = time.time() - start_time
        result["duration_seconds"] = duration
        
        if result["success"]:
            self.logger.log_snapshot_complete(
                snapshot_id=snapshot_id,
                record_count=result["records_written"],
                duration_seconds=duration,
            )
            
            # Update metrics
            self.metrics["total_snapshots"] += 1
            self.metrics["total_records_processed"] += result["records_consumed"]
            self.metrics["total_records_written"] += result["records_written"]
            self.metrics["total_invalid_records"] += result["records_invalid"]
            self.metrics["last_snapshot_time"] = datetime.utcnow().isoformat()
            self.metrics["last_snapshot_duration"] = duration
        
        return result
    
    def _generate_snapshot_id(self) -> str:
        """
        Generate unique snapshot ID.
        
        Returns:
            Unique snapshot identifier
        """
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"snapshot_{timestamp}_{unique_id}"
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get current metrics.
        
        Returns:
            Dictionary of metrics
        """
        return self.metrics.copy()
    
    def run_once(self) -> Dict[str, Any]:
        """
        Run a single snapshot capture (for testing or manual execution).
        
        Returns:
            Snapshot result metrics
        """
        self.logger.info("Running single snapshot capture")
        
        # Connect if not already connected
        if not self.consumer._is_connected:
            self.consumer.connect()
        if not self.writer._is_connected:
            self.writer.connect()
        
        result = self.capture_snapshot()
        
        return result
    
    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()
        return False
