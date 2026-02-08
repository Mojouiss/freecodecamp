"""
Kafka consumer for financial position messages using confluent-kafka.

Provides robust, efficient Kafka consumption with error handling and batching.
"""

from typing import List, Dict, Any, Optional, Callable
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
import json
from datetime import datetime
import time
import pandas as pd

from ..core.config import KafkaConfig
from ..utils.logger import SnapshotLogger


class FinancialPositionConsumer:
    """
    Kafka consumer for financial position records using confluent-kafka.
    
    Handles message consumption, deserialization, and batch processing
    with robust error handling and performance optimization.
    """
    
    def __init__(
        self,
        config: KafkaConfig,
        logger: Optional[SnapshotLogger] = None,
        value_deserializer: Optional[Callable] = None,
    ):
        """
        Initialize the Kafka consumer.
        
        Args:
            config: Kafka configuration
            logger: Logger instance
            value_deserializer: Custom value deserializer function
        """
        self.config = config
        self.logger = logger or SnapshotLogger("FinancialPositionConsumer")
        
        # Default JSON deserializer
        if value_deserializer is None:
            value_deserializer = lambda m: json.loads(m.decode("utf-8"))
        
        self.consumer: Optional[Consumer] = None
        self.value_deserializer = value_deserializer
        self._is_connected = False
    
    def connect(self) -> None:
        """
        Establish connection to Kafka.
        
        Raises:
            KafkaException: If connection fails
        """
        try:
            self.logger.info(
                "Connecting to Kafka",
                bootstrap_servers=self.config.bootstrap_servers,
                topic=self.config.topic,
            )
            
            # Get confluent-kafka config
            consumer_config = self.config.to_confluent_config()
            
            # Create consumer
            self.consumer = Consumer(consumer_config)
            
            # Subscribe to topic
            self.consumer.subscribe([self.config.topic])
            
            self._is_connected = True
            self.logger.info("Successfully connected to Kafka")
            
        except KafkaException as e:
            self.logger.error("Failed to connect to Kafka", error=str(e))
            raise
    
    def disconnect(self) -> None:
        """Disconnect from Kafka."""
        if self.consumer:
            self.logger.info("Disconnecting from Kafka")
            self.consumer.close()
            self._is_connected = False
            self.logger.info("Disconnected from Kafka")
    
    def consume_batch(
        self,
        timeout_ms: int = 10000,
        max_records: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Consume a batch of records from Kafka.
        
        Args:
            timeout_ms: Timeout for polling in milliseconds
            max_records: Maximum number of records to consume (None for config default)
            
        Returns:
            List of deserialized records
            
        Raises:
            RuntimeError: If consumer is not connected
        """
        if not self._is_connected or not self.consumer:
            raise RuntimeError("Consumer is not connected. Call connect() first.")
        
        records = []
        start_time = time.time()
        max_msgs = max_records or self.config.max_poll_records
        timeout_sec = timeout_ms / 1000.0
        
        try:
            # Poll for messages
            while len(records) < max_msgs:
                elapsed = time.time() - start_time
                if elapsed >= timeout_sec:
                    break
                
                remaining_timeout = timeout_sec - elapsed
                msg = self.consumer.poll(timeout=remaining_timeout)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition - normal behavior
                        continue
                    else:
                        self.logger.error(
                            "Kafka error during consumption",
                            error=str(msg.error())
                        )
                        raise KafkaException(msg.error())
                
                try:
                    # Deserialize and create record
                    value = self.value_deserializer(msg.value())
                    
                    record = {
                        "value": value,
                        "key": msg.key().decode("utf-8") if msg.key() else None,
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "timestamp": msg.timestamp()[1] if msg.timestamp()[0] > 0 else None,
                        "consumed_at": datetime.utcnow().isoformat(),
                    }
                    records.append(record)
                    
                except Exception as e:
                    self.logger.error(
                        "Failed to process message",
                        partition=msg.partition(),
                        offset=msg.offset(),
                        error=str(e),
                    )
            
            # Log consumption metrics
            if records:
                # Group by partition for logging
                partitions = {}
                for record in records:
                    p = record["partition"]
                    if p not in partitions:
                        partitions[p] = []
                    partitions[p].append(record)
                
                for partition, partition_records in partitions.items():
                    last_offset = partition_records[-1]["offset"]
                    self.logger.log_kafka_consumption(
                        topic=self.config.topic,
                        partition=partition,
                        offset=last_offset,
                        record_count=len(partition_records),
                    )
            
            duration = time.time() - start_time
            if records:
                self.logger.debug(
                    "Consumed batch",
                    record_count=len(records),
                    duration_seconds=f"{duration:.2f}",
                )
            
            return records
            
        except KafkaException as e:
            self.logger.error("Kafka error during consumption", error=str(e))
            raise
    
    def consume_batch_as_dataframe(
        self,
        timeout_ms: int = 10000,
        max_records: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Consume a batch of records and return as pandas DataFrame.
        
        Args:
            timeout_ms: Timeout for polling in milliseconds
            max_records: Maximum number of records to consume
            
        Returns:
            DataFrame with consumed records
        """
        records = self.consume_batch(timeout_ms=timeout_ms, max_records=max_records)
        
        if not records:
            # Return empty DataFrame with expected schema
            return pd.DataFrame(columns=[
                "key", "partition", "offset", "timestamp", "consumed_at",
                "position_id", "account_id", "symbol", "quantity",
                "market_value", "cost_basis", "unrealized_pnl"
            ])
        
        # Flatten records for DataFrame
        data = []
        for record in records:
            value = record.get("value", {})
            row = {
                "key": record.get("key"),
                "partition": record.get("partition"),
                "offset": record.get("offset"),
                "timestamp": record.get("timestamp"),
                "consumed_at": record.get("consumed_at"),
                "position_id": value.get("position_id"),
                "account_id": value.get("account_id"),
                "symbol": value.get("symbol"),
                "quantity": value.get("quantity"),
                "market_value": value.get("market_value"),
                "cost_basis": value.get("cost_basis"),
                "unrealized_pnl": value.get("unrealized_pnl"),
            }
            data.append(row)
        
        df = pd.DataFrame(data)
        
        # Convert data types
        numeric_cols = ["quantity", "market_value", "cost_basis", "unrealized_pnl"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        return df
    
    def commit(self, asynchronous: bool = True) -> None:
        """
        Manually commit offsets.
        
        Should be called after successfully processing and storing records.
        
        Args:
            asynchronous: Whether to commit asynchronously
        """
        if not self._is_connected or not self.consumer:
            raise RuntimeError("Consumer is not connected")
        
        try:
            self.consumer.commit(asynchronous=asynchronous)
            self.logger.debug("Committed offsets to Kafka")
        except KafkaException as e:
            self.logger.error("Failed to commit offsets", error=str(e))
            raise
    
    def seek_to_beginning(self) -> None:
        """Seek to the beginning of all assigned partitions."""
        if not self._is_connected or not self.consumer:
            raise RuntimeError("Consumer is not connected")
        
        partitions = self.consumer.assignment()
        if partitions:
            for partition in partitions:
                self.consumer.seek(TopicPartition(
                    partition.topic,
                    partition.partition,
                    0
                ))
            self.logger.info("Seeked to beginning of partitions")
    
    def seek_to_end(self) -> None:
        """Seek to the end of all assigned partitions."""
        if not self._is_connected or not self.consumer:
            raise RuntimeError("Consumer is not connected")
        
        partitions = self.consumer.assignment()
        if partitions:
            for partition in partitions:
                low, high = self.consumer.get_watermark_offsets(partition)
                self.consumer.seek(TopicPartition(
                    partition.topic,
                    partition.partition,
                    high
                ))
            self.logger.info("Seeked to end of partitions")
    
    def get_current_offsets(self) -> Dict[int, int]:
        """
        Get current offsets for all assigned partitions.
        
        Returns:
            Dictionary mapping partition to current offset
        """
        if not self._is_connected or not self.consumer:
            raise RuntimeError("Consumer is not connected")
        
        offsets = {}
        partitions = self.consumer.assignment()
        
        if partitions:
            for partition in partitions:
                position = self.consumer.position([partition])
                if position:
                    offsets[partition.partition] = position[0].offset
        
        return offsets
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        return False
