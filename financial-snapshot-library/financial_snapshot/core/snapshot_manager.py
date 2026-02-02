"""
Main snapshot manager orchestrating the entire snapshot process.

Coordinates Kafka consumption, data processing, and SQL storage with
continuous consumption, periodic snapshots, and proper offset management.
"""

from typing import Optional, Dict, Any, List
import time
from datetime import datetime
import uuid
from threading import Event, Thread, Lock

from .config import Config
from ..messaging.kafka_consumer import FinancialPositionConsumer
from ..storage.sql_writer import FinancialPositionWriter
from ..utils.logger import SnapshotLogger
from ..utils.data_utils import Deduplicator, DataValidator


class SnapshotManager:
    """
    Main orchestrator for financial snapshot operations.
    
    Architecture: Continuously consumes from Kafka → buffers records → 
    every 5 minutes flushes buffer to SQL → commits offsets.
    
    Key features:
    - Continuous Kafka consumption in dedicated thread
    - Periodic snapshot flushing (default 5 minutes)
    - Transactional writes with offset commit only after DB success
    - Thread-safe buffer management
    - No data loss guarantee
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
        
        # Buffer for continuous consumption
        self._buffer: List[Dict[str, Any]] = []
        self._buffer_lock = Lock()
        
        # Threading
        self._stop_event = Event()
        self._consumer_thread: Optional[Thread] = None
        self._snapshot_thread: Optional[Thread] = None
        
        # Metrics
        self.metrics: Dict[str, Any] = {
            "total_snapshots": 0,
            "total_records_consumed": 0,
            "total_records_processed": 0,
            "total_records_written": 0,
            "total_invalid_records": 0,
            "total_errors": 0,
            "last_snapshot_time": None,
            "last_snapshot_duration": None,
            "buffer_size": 0,
        }
    
    def start(self) -> None:
        """
        Start the snapshot manager with continuous consumption.
        
        Starts two threads:
        1. Consumer thread: Continuously consumes from Kafka and buffers records
        2. Snapshot thread: Flushes buffer to SQL every configured interval
        """
        self.logger.info(
            "Starting snapshot manager with continuous consumption",
            interval_seconds=self.config.snapshot.interval_seconds,
        )
        
        # Connect to Kafka and database
        self.consumer.connect()
        self.writer.connect()
        
        # Clear stop event and buffer
        self._stop_event.clear()
        with self._buffer_lock:
            self._buffer.clear()
        
        # Start consumer thread (continuously consumes and buffers)
        self._consumer_thread = Thread(target=self._run_consumer, daemon=True)
        self._consumer_thread.start()
        
        # Start snapshot thread (flushes buffer periodically)
        self._snapshot_thread = Thread(target=self._run_snapshot_scheduler, daemon=True)
        self._snapshot_thread.start()
        
        self.logger.info("Snapshot manager started successfully")
    
    def stop(self) -> None:
        """Stop the snapshot manager gracefully."""
        self.logger.info("Stopping snapshot manager")
        
        # Signal threads to stop
        self._stop_event.set()
        
        # Wait for threads to finish
        if self._consumer_thread:
            self._consumer_thread.join(timeout=10)
        if self._snapshot_thread:
            self._snapshot_thread.join(timeout=10)
        
        # Flush any remaining buffered records
        self.logger.info("Flushing remaining buffered records")
        try:
            self._flush_buffer()
        except Exception as e:
            self.logger.error("Error flushing final buffer", error=str(e))
        
        # Disconnect
        self.consumer.disconnect()
        self.writer.disconnect()
        
        self.logger.info("Snapshot manager stopped")
    
    def _run_consumer(self) -> None:
        """
        Consumer thread: Continuously consume messages from Kafka and buffer them.
        
        This thread runs continuously until stopped, polling Kafka and adding
        messages to the buffer for later processing.
        """
        self.logger.info("Consumer thread started")
        
        consecutive_empty_polls = 0
        max_empty_polls = 10
        
        while not self._stop_event.is_set():
            try:
                # Poll Kafka for messages (short timeout for responsiveness)
                records = self.consumer.consume_batch(
                    timeout_ms=1000,  # 1 second timeout
                    max_records=self.config.kafka.max_poll_records,
                )
                
                if records:
                    # Add to buffer
                    with self._buffer_lock:
                        self._buffer.extend(records)
                        buffer_size = len(self._buffer)
                    
                    self.logger.debug(
                        "Consumed and buffered records",
                        count=len(records),
                        buffer_size=buffer_size,
                    )
                    self.metrics["total_records_consumed"] += len(records)
                    self.metrics["buffer_size"] = buffer_size
                    consecutive_empty_polls = 0
                else:
                    consecutive_empty_polls += 1
                    
                    # If no messages for a while, log it
                    if consecutive_empty_polls >= max_empty_polls:
                        self.logger.debug("No messages received from Kafka")
                        consecutive_empty_polls = 0
                
            except Exception as e:
                self.logger.error(
                    "Error in consumer thread",
                    error=str(e),
                    error_type=type(e).__name__,
                )
                self.metrics["total_errors"] += 1
                
                # Brief pause before retrying
                time.sleep(self.config.snapshot.retry_backoff_seconds)
        
        self.logger.info("Consumer thread stopped")
    
    def _run_snapshot_scheduler(self) -> None:
        """
        Snapshot thread: Flush buffer to SQL every interval.
        
        This thread runs on a timer, periodically flushing the buffered
        records to the database and committing Kafka offsets.
        """
        self.logger.info("Snapshot scheduler thread started")
        
        while not self._stop_event.is_set():
            try:
                # Wait for the snapshot interval
                self._stop_event.wait(self.config.snapshot.interval_seconds)
                
                if not self._stop_event.is_set():
                    # Flush buffer to database
                    self._flush_buffer()
                
            except Exception as e:
                self.logger.error(
                    "Unexpected error in snapshot scheduler",
                    error=str(e),
                    error_type=type(e).__name__,
                )
                self.metrics["total_errors"] += 1
                
                # Wait before retrying
                time.sleep(self.config.snapshot.retry_backoff_seconds)
        
        self.logger.info("Snapshot scheduler thread stopped")
    
    def _flush_buffer(self) -> Dict[str, Any]:
        """
        Flush buffered records to database and commit offsets.
        
        This is the core snapshot operation:
        1. Get all buffered records (thread-safe)
        2. Validate records
        3. Deduplicate if enabled
        4. Write to database in single transaction
        5. Commit Kafka offsets only after successful write
        6. Clear buffer
        
        Returns:
            Dictionary containing snapshot metrics
        """
        snapshot_id = self._generate_snapshot_id()
        start_time = time.time()
        
        # Get buffered records and clear buffer (atomic operation)
        with self._buffer_lock:
            records = self._buffer.copy()
            buffer_size_before = len(self._buffer)
        
        if not records:
            self.logger.debug("No buffered records to flush")
            return {
                "snapshot_id": snapshot_id,
                "success": True,
                "records_buffered": 0,
                "records_written": 0,
                "duration_seconds": 0,
            }
        
        self.logger.log_snapshot_start(snapshot_id)
        self.logger.info(
            "Flushing buffered records",
            snapshot_id=snapshot_id,
            buffer_size=len(records),
        )
        
        result = {
            "snapshot_id": snapshot_id,
            "success": False,
            "records_buffered": len(records),
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
                
                # Write to database in single transaction
                if valid_records:
                    records_written = self.writer.write_batch(
                        records=valid_records,
                        snapshot_id=snapshot_id,
                    )
                    result["records_written"] = records_written
                    
                    self.logger.info(
                        "Successfully wrote records to database",
                        snapshot_id=snapshot_id,
                        records_written=records_written,
                    )
                
                # CRITICAL: Commit Kafka offsets ONLY after successful DB write
                # This ensures exactly-once semantics and no data loss
                self.consumer.commit()
                self.logger.info("Committed Kafka offsets after successful write")
                
                # Clear buffer now that write and commit succeeded
                with self._buffer_lock:
                    self._buffer.clear()
                    self.metrics["buffer_size"] = 0
                
                result["success"] = True
                break
                
            except Exception as e:
                retry_count += 1
                result["error"] = str(e)
                
                if retry_count <= max_retries:
                    self.logger.log_error_with_retry(
                        operation="flush_buffer",
                        error=e,
                        retry_count=retry_count,
                        max_retries=max_retries,
                    )
                    time.sleep(self.config.snapshot.retry_backoff_seconds)
                else:
                    self.logger.error(
                        "Max retries exceeded for snapshot flush",
                        snapshot_id=snapshot_id,
                        error=str(e),
                    )
                    self.metrics["total_errors"] += 1
                    
                    # NOTE: Buffer is NOT cleared on failure
                    # Records remain in buffer for next flush attempt
                    self.logger.warning(
                        "Records remain in buffer due to failure",
                        buffer_size=buffer_size_before,
                    )
        
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
            self.metrics["total_records_processed"] += result["records_buffered"]
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
            Dictionary of metrics including current buffer size
        """
        with self._buffer_lock:
            self.metrics["buffer_size"] = len(self._buffer)
        return self.metrics.copy()
    
    def get_buffer_size(self) -> int:
        """
        Get current buffer size.
        
        Returns:
            Number of records currently buffered
        """
        with self._buffer_lock:
            return len(self._buffer)
    
    def run_once(self) -> Dict[str, Any]:
        """
        Run a single snapshot flush (for testing or manual execution).
        
        Note: This does not start continuous consumption. Use start() for that.
        
        Returns:
            Snapshot result metrics
        """
        self.logger.info("Running single snapshot flush")
        
        # Connect if not already connected
        if not self.consumer._is_connected:
            self.consumer.connect()
        if not self.writer._is_connected:
            self.writer.connect()
        
        # Consume a batch and add to buffer
        records = self.consumer.consume_batch(
            timeout_ms=self.config.snapshot.processing_timeout_seconds * 1000,
        )
        
        with self._buffer_lock:
            self._buffer.extend(records)
        
        # Flush the buffer
        result = self._flush_buffer()
        
        return result
    
    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()
        return False
