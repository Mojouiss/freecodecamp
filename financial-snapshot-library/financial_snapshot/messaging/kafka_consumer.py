"""
Kafka consumer for financial position messages.

Provides robust, efficient Kafka consumption with error handling and batching.
"""

from typing import List, Dict, Any, Optional, Callable
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
from datetime import datetime
import time

from ..core.config import KafkaConfig
from ..utils.logger import SnapshotLogger


class FinancialPositionConsumer:
    """
    Kafka consumer for financial position records.
    
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
        
        self.consumer: Optional[KafkaConsumer] = None
        self.value_deserializer = value_deserializer
        self._is_connected = False
    
    def connect(self) -> None:
        """
        Establish connection to Kafka.
        
        Raises:
            KafkaError: If connection fails
        """
        try:
            self.logger.info(
                "Connecting to Kafka",
                bootstrap_servers=self.config.bootstrap_servers,
                topic=self.config.topic,
            )
            
            consumer_config = self.config.to_dict()
            consumer_config["value_deserializer"] = self.value_deserializer
            
            self.consumer = KafkaConsumer(
                self.config.topic,
                **consumer_config,
            )
            
            self._is_connected = True
            self.logger.info("Successfully connected to Kafka")
            
        except KafkaError as e:
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
        
        try:
            # Poll for messages
            messages = self.consumer.poll(
                timeout_ms=timeout_ms,
                max_records=max_records or self.config.max_poll_records,
            )
            
            # Extract and transform messages
            for topic_partition, msgs in messages.items():
                for msg in msgs:
                    try:
                        record = {
                            "value": msg.value,
                            "key": msg.key.decode("utf-8") if msg.key else None,
                            "partition": msg.partition,
                            "offset": msg.offset,
                            "timestamp": msg.timestamp,
                            "consumed_at": datetime.utcnow().isoformat(),
                        }
                        records.append(record)
                        
                    except Exception as e:
                        self.logger.error(
                            "Failed to process message",
                            partition=msg.partition,
                            offset=msg.offset,
                            error=str(e),
                        )
                
                if records:
                    last_msg = msgs[-1]
                    self.logger.log_kafka_consumption(
                        topic=topic_partition.topic,
                        partition=topic_partition.partition,
                        offset=last_msg.offset,
                        record_count=len(msgs),
                    )
            
            duration = time.time() - start_time
            if records:
                self.logger.debug(
                    "Consumed batch",
                    record_count=len(records),
                    duration_seconds=f"{duration:.2f}",
                )
            
            return records
            
        except KafkaError as e:
            self.logger.error("Kafka error during consumption", error=str(e))
            raise
    
    def commit(self) -> None:
        """
        Manually commit offsets.
        
        Should be called after successfully processing and storing records.
        """
        if not self._is_connected or not self.consumer:
            raise RuntimeError("Consumer is not connected")
        
        try:
            self.consumer.commit()
            self.logger.debug("Committed offsets to Kafka")
        except KafkaError as e:
            self.logger.error("Failed to commit offsets", error=str(e))
            raise
    
    def seek_to_beginning(self) -> None:
        """Seek to the beginning of all assigned partitions."""
        if not self._is_connected or not self.consumer:
            raise RuntimeError("Consumer is not connected")
        
        self.consumer.seek_to_beginning()
        self.logger.info("Seeked to beginning of partitions")
    
    def seek_to_end(self) -> None:
        """Seek to the end of all assigned partitions."""
        if not self._is_connected or not self.consumer:
            raise RuntimeError("Consumer is not connected")
        
        self.consumer.seek_to_end()
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
        for partition in self.consumer.assignment():
            position = self.consumer.position(partition)
            offsets[partition.partition] = position
        
        return offsets
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        return False
