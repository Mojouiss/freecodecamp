"""
SQL database writer for financial positions.

Provides efficient, reliable storage with connection pooling, batch inserts,
and transaction management.
"""

from typing import List, Dict, Any, Optional
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime,
    MetaData,
    Index,
)
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import time

from ..core.config import DatabaseConfig
from ..utils.logger import SnapshotLogger


class FinancialPositionWriter:
    """
    SQL database writer for financial position records.
    
    Handles batch inserts, connection pooling, and transaction management
    for optimal database performance and reliability.
    """
    
    def __init__(
        self,
        config: DatabaseConfig,
        logger: Optional[SnapshotLogger] = None,
    ):
        """
        Initialize the database writer.
        
        Args:
            config: Database configuration
            logger: Logger instance
        """
        self.config = config
        self.logger = logger or SnapshotLogger("FinancialPositionWriter")
        
        self.engine: Optional[Engine] = None
        self.metadata = MetaData()
        self.table: Optional[Table] = None
        self._is_connected = False
    
    def connect(self) -> None:
        """
        Establish database connection with connection pooling.
        
        Raises:
            SQLAlchemyError: If connection fails
        """
        try:
            self.logger.info(
                "Connecting to database",
                driver=self.config.driver,
                host=self.config.host,
                database=self.config.database,
            )
            
            # Create engine with connection pooling
            self.engine = create_engine(
                self.config.get_connection_string(),
                poolclass=QueuePool,
                pool_size=self.config.pool_size,
                max_overflow=self.config.max_overflow,
                pool_timeout=self.config.pool_timeout,
                pool_recycle=self.config.pool_recycle,
                pool_pre_ping=True,  # Verify connections before using
            )
            
            # Define table schema
            self._define_table()
            
            # Create table if it doesn't exist
            self.metadata.create_all(self.engine)
            
            self._is_connected = True
            self.logger.info("Successfully connected to database")
            
        except SQLAlchemyError as e:
            self.logger.error("Failed to connect to database", error=str(e))
            raise
    
    def _define_table(self) -> None:
        """Define the financial positions table schema."""
        self.table = Table(
            self.config.table_name,
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("snapshot_id", String(100), nullable=False, index=True),
            Column("position_id", String(100), nullable=False, index=True),
            Column("account_id", String(100), nullable=False, index=True),
            Column("symbol", String(50), nullable=False),
            Column("quantity", Float, nullable=False),
            Column("market_value", Float, nullable=True),
            Column("cost_basis", Float, nullable=True),
            Column("unrealized_pnl", Float, nullable=True),
            Column("kafka_partition", Integer, nullable=True),
            Column("kafka_offset", Integer, nullable=True),
            Column("kafka_timestamp", DateTime, nullable=True),
            Column("consumed_at", DateTime, nullable=True),
            Column("created_at", DateTime, default=datetime.utcnow, nullable=False),
            
            # Indexes for common queries
            Index(
                f"idx_{self.config.table_name}_snapshot_account",
                "snapshot_id",
                "account_id",
            ),
            Index(
                f"idx_{self.config.table_name}_created_at",
                "created_at",
            ),
        )
    
    def disconnect(self) -> None:
        """Disconnect from database."""
        if self.engine:
            self.logger.info("Disconnecting from database")
            self.engine.dispose()
            self._is_connected = False
            self.logger.info("Disconnected from database")
    
    def write_batch(
        self,
        records: List[Dict[str, Any]],
        snapshot_id: str,
    ) -> int:
        """
        Write a batch of records to the database.
        
        Args:
            records: List of financial position records
            snapshot_id: Unique identifier for this snapshot
            
        Returns:
            Number of records written
            
        Raises:
            RuntimeError: If writer is not connected
            SQLAlchemyError: If write operation fails
        """
        if not self._is_connected or not self.engine or not self.table:
            raise RuntimeError("Writer is not connected. Call connect() first.")
        
        if not records:
            self.logger.debug("No records to write")
            return 0
        
        start_time = time.time()
        total_written = 0
        
        try:
            # Process in batches
            batch_size = self.config.batch_size
            
            with self.engine.begin() as connection:
                for i in range(0, len(records), batch_size):
                    batch = records[i:i + batch_size]
                    
                    # Transform records for insertion
                    insert_data = []
                    for record in batch:
                        value = record.get("value", {})
                        
                        row = {
                            "snapshot_id": snapshot_id,
                            "position_id": value.get("position_id", ""),
                            "account_id": value.get("account_id", ""),
                            "symbol": value.get("symbol", ""),
                            "quantity": float(value.get("quantity", 0)),
                            "market_value": float(value.get("market_value")) if value.get("market_value") is not None else None,
                            "cost_basis": float(value.get("cost_basis")) if value.get("cost_basis") is not None else None,
                            "unrealized_pnl": float(value.get("unrealized_pnl")) if value.get("unrealized_pnl") is not None else None,
                            "kafka_partition": record.get("partition"),
                            "kafka_offset": record.get("offset"),
                            "kafka_timestamp": datetime.fromtimestamp(record.get("timestamp", 0) / 1000) if record.get("timestamp") else None,
                            "consumed_at": datetime.fromisoformat(record.get("consumed_at")) if record.get("consumed_at") else None,
                        }
                        insert_data.append(row)
                    
                    # Execute batch insert
                    connection.execute(self.table.insert(), insert_data)
                    total_written += len(insert_data)
                    
                    self.logger.debug(
                        "Batch inserted",
                        batch_number=i // batch_size + 1,
                        record_count=len(insert_data),
                    )
            
            duration = time.time() - start_time
            self.logger.log_database_write(
                table=self.config.table_name,
                record_count=total_written,
                duration_seconds=duration,
            )
            
            return total_written
            
        except SQLAlchemyError as e:
            self.logger.error(
                "Failed to write batch to database",
                record_count=len(records),
                error=str(e),
            )
            raise
    
    def get_latest_snapshot_id(self) -> Optional[str]:
        """
        Get the most recent snapshot ID from the database.
        
        Returns:
            Latest snapshot ID or None if table is empty
        """
        if not self._is_connected or not self.engine or not self.table:
            raise RuntimeError("Writer is not connected")
        
        try:
            with self.engine.connect() as connection:
                query = self.table.select().order_by(
                    self.table.c.created_at.desc()
                ).limit(1)
                result = connection.execute(query).fetchone()
                
                if result:
                    return result.snapshot_id
                return None
                
        except SQLAlchemyError as e:
            self.logger.error("Failed to get latest snapshot ID", error=str(e))
            raise
    
    def count_records(self, snapshot_id: Optional[str] = None) -> int:
        """
        Count records in the database.
        
        Args:
            snapshot_id: Optional snapshot ID to filter by
            
        Returns:
            Number of records
        """
        if not self._is_connected or not self.engine or not self.table:
            raise RuntimeError("Writer is not connected")
        
        try:
            with self.engine.connect() as connection:
                from sqlalchemy import func, select
                
                query = select(func.count()).select_from(self.table)
                if snapshot_id:
                    query = query.where(self.table.c.snapshot_id == snapshot_id)
                
                result = connection.execute(query).scalar()
                return result or 0
                
        except SQLAlchemyError as e:
            self.logger.error("Failed to count records", error=str(e))
            raise
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        return False
