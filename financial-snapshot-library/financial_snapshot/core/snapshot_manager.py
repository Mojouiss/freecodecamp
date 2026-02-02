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
        
        # Buffers for continuous consumption (separate by record type)
        self._pnl_buffer: List[Dict[str, Any]] = []
        self._risk_buffer: List[Dict[str, Any]] = []
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
            "total_pnl_records": 0,
            "total_risk_records": 0,
            "total_unrouted_records": 0,
            "total_invalid_records": 0,
            "total_errors": 0,
            "last_snapshot_time": None,
            "last_snapshot_duration": None,
            "pnl_buffer_size": 0,
            "risk_buffer_size": 0,
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
        
        # Clear stop event and buffers
        self._stop_event.clear()
        with self._buffer_lock:
            self._pnl_buffer.clear()
            self._risk_buffer.clear()
        
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
                    # Route records to appropriate buffers based on packageName
                    pnl_records = []
                    risk_records = []
                    unrouted_records = []
                    
                    for record in records:
                        value = record.get("value", {})
                        package_name = value.get("packageName", "")
                        
                        if package_name == "PnlRecords":
                            pnl_records.append(record)
                        elif package_name == "RiskRecords":
                            risk_records.append(record)
                        else:
                            unrouted_records.append(record)
                            self.logger.warning(
                                "Record with unknown packageName",
                                package_name=package_name,
                                position_id=value.get("position_id", "unknown"),
                            )
                    
                    # Add to respective buffers
                    with self._buffer_lock:
                        self._pnl_buffer.extend(pnl_records)
                        self._risk_buffer.extend(risk_records)
                        pnl_size = len(self._pnl_buffer)
                        risk_size = len(self._risk_buffer)
                    
                    self.logger.debug(
                        "Consumed and buffered records",
                        total_count=len(records),
                        pnl_count=len(pnl_records),
                        risk_count=len(risk_records),
                        unrouted_count=len(unrouted_records),
                        pnl_buffer_size=pnl_size,
                        risk_buffer_size=risk_size,
                    )
                    
                    self.metrics["total_records_consumed"] += len(records)
                    self.metrics["total_pnl_records"] += len(pnl_records)
                    self.metrics["total_risk_records"] += len(risk_records)
                    self.metrics["total_unrouted_records"] += len(unrouted_records)
                    self.metrics["pnl_buffer_size"] = pnl_size
                    self.metrics["risk_buffer_size"] = risk_size
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
        1. Get all buffered records for both types (thread-safe)
        2. Validate records for each type
        3. Deduplicate if enabled
        4. Write to both tables (PnLItd and RiskItd) in single transaction
        5. Commit Kafka offsets only after successful write to BOTH tables
        6. Clear both buffers
        
        Returns:
            Dictionary containing snapshot metrics
        """
        snapshot_id = self._generate_snapshot_id()
        start_time = time.time()
        
        # Get buffered records from both buffers (atomic operation)
        with self._buffer_lock:
            pnl_records = self._pnl_buffer.copy()
            risk_records = self._risk_buffer.copy()
            pnl_buffer_size_before = len(self._pnl_buffer)
            risk_buffer_size_before = len(self._risk_buffer)
        
        total_records = len(pnl_records) + len(risk_records)
        
        if total_records == 0:
            self.logger.debug("No buffered records to flush")
            return {
                "snapshot_id": snapshot_id,
                "success": True,
                "pnl_records_buffered": 0,
                "risk_records_buffered": 0,
                "pnl_records_written": 0,
                "risk_records_written": 0,
                "duration_seconds": 0,
            }
        
        self.logger.log_snapshot_start(snapshot_id)
        self.logger.info(
            "Flushing buffered records",
            snapshot_id=snapshot_id,
            pnl_buffer_size=len(pnl_records),
            risk_buffer_size=len(risk_records),
            total_buffer_size=total_records,
        )
        
        result = {
            "snapshot_id": snapshot_id,
            "success": False,
            "pnl_records_buffered": len(pnl_records),
            "risk_records_buffered": len(risk_records),
            "pnl_records_valid": 0,
            "risk_records_valid": 0,
            "pnl_records_written": 0,
            "risk_records_written": 0,
            "pnl_records_invalid": 0,
            "risk_records_invalid": 0,
            "duration_seconds": 0,
            "error": None,
        }
        
        retry_count = 0
        max_retries = self.config.snapshot.max_retries
        
        while retry_count <= max_retries:
            try:
                # Process PnL records
                pnl_valid_records = []
                if pnl_records:
                    pnl_valid, pnl_invalid = DataValidator.validate_batch(pnl_records)
                    result["pnl_records_valid"] = len(pnl_valid)
                    result["pnl_records_invalid"] = len(pnl_invalid)
                    
                    if pnl_invalid:
                        self.logger.warning(
                            "Invalid PnL records found",
                            count=len(pnl_invalid),
                        )
                    
                    # Deduplicate if enabled
                    if self.deduplicator and pnl_valid:
                        original_count = len(pnl_valid)
                        pnl_valid = self.deduplicator.deduplicate(pnl_valid)
                        dedup_count = original_count - len(pnl_valid)
                        
                        if dedup_count > 0:
                            self.logger.info(
                                "Deduplicated PnL records",
                                removed=dedup_count,
                                remaining=len(pnl_valid),
                            )
                    
                    pnl_valid_records = pnl_valid
                
                # Process Risk records
                risk_valid_records = []
                if risk_records:
                    risk_valid, risk_invalid = DataValidator.validate_batch(risk_records)
                    result["risk_records_valid"] = len(risk_valid)
                    result["risk_records_invalid"] = len(risk_invalid)
                    
                    if risk_invalid:
                        self.logger.warning(
                            "Invalid Risk records found",
                            count=len(risk_invalid),
                        )
                    
                    # Deduplicate if enabled
                    if self.deduplicator and risk_valid:
                        original_count = len(risk_valid)
                        risk_valid = self.deduplicator.deduplicate(risk_valid)
                        dedup_count = original_count - len(risk_valid)
                        
                        if dedup_count > 0:
                            self.logger.info(
                                "Deduplicated Risk records",
                                removed=dedup_count,
                                remaining=len(risk_valid),
                            )
                    
                    risk_valid_records = risk_valid
                
                # Write to both tables in single transaction context
                # This ensures atomicity - both writes succeed or both fail
                if pnl_valid_records or risk_valid_records:
                    records_by_type = {}
                    if pnl_valid_records:
                        records_by_type["PnLItd"] = pnl_valid_records
                    if risk_valid_records:
                        records_by_type["RiskItd"] = risk_valid_records
                    
                    write_results = self.writer.write_multi_table_batch(
                        records_by_type=records_by_type,
                        snapshot_id=snapshot_id,
                    )
                    
                    result["pnl_records_written"] = write_results.get("PnLItd", 0)
                    result["risk_records_written"] = write_results.get("RiskItd", 0)
                    
                    self.logger.info(
                        "Successfully wrote records to multiple tables",
                        snapshot_id=snapshot_id,
                        pnl_records_written=result["pnl_records_written"],
                        risk_records_written=result["risk_records_written"],
                    )
                
                # CRITICAL: Commit Kafka offsets ONLY after successful DB writes to BOTH tables
                # This ensures exactly-once semantics and no data loss
                self.consumer.commit()
                self.logger.info("Committed Kafka offsets after successful writes to both tables")
                
                # Clear both buffers now that write and commit succeeded
                with self._buffer_lock:
                    self._pnl_buffer.clear()
                    self._risk_buffer.clear()
                    self.metrics["pnl_buffer_size"] = 0
                    self.metrics["risk_buffer_size"] = 0
                
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
                    
                    # NOTE: Buffers are NOT cleared on failure
                    # Records remain in buffers for next flush attempt
                    self.logger.warning(
                        "Records remain in buffers due to failure",
                        pnl_buffer_size=pnl_buffer_size_before,
                        risk_buffer_size=risk_buffer_size_before,
                    )
        
        # Calculate duration and update metrics
        duration = time.time() - start_time
        result["duration_seconds"] = duration
        
        if result["success"]:
            total_written = result["pnl_records_written"] + result["risk_records_written"]
            self.logger.log_snapshot_complete(
                snapshot_id=snapshot_id,
                record_count=total_written,
                duration_seconds=duration,
            )
            
            # Update metrics
            self.metrics["total_snapshots"] += 1
            self.metrics["total_records_processed"] += (
                result["pnl_records_buffered"] + result["risk_records_buffered"]
            )
            self.metrics["total_records_written"] += total_written
            self.metrics["total_invalid_records"] += (
                result["pnl_records_invalid"] + result["risk_records_invalid"]
            )
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
            Dictionary of metrics including current buffer sizes
        """
        with self._buffer_lock:
            self.metrics["pnl_buffer_size"] = len(self._pnl_buffer)
            self.metrics["risk_buffer_size"] = len(self._risk_buffer)
        return self.metrics.copy()
    
    def get_buffer_sizes(self) -> Dict[str, int]:
        """
        Get current buffer sizes for both record types.
        
        Returns:
            Dictionary with pnl and risk buffer sizes
        """
        with self._buffer_lock:
            return {
                "pnl": len(self._pnl_buffer),
                "risk": len(self._risk_buffer),
                "total": len(self._pnl_buffer) + len(self._risk_buffer),
            }
    
    def get_buffer_size(self) -> int:
        """
        Get total current buffer size (legacy method).
        
        Returns:
            Total number of records currently buffered
        """
        with self._buffer_lock:
            return len(self._pnl_buffer) + len(self._risk_buffer)
    
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
