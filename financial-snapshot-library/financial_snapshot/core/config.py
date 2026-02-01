"""
Configuration management for the financial snapshot library.

This module provides a robust configuration system with validation,
environment variable support, and sensible defaults.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional
import os
from pathlib import Path
import json


@dataclass
class KafkaConfig:
    """Kafka consumer configuration."""
    
    bootstrap_servers: str = field(default="localhost:9092")
    topic: str = field(default="financial_positions")
    group_id: str = field(default="financial_snapshot_consumer")
    auto_offset_reset: str = field(default="earliest")
    enable_auto_commit: bool = field(default=False)
    max_poll_records: int = field(default=500)
    session_timeout_ms: int = field(default=30000)
    max_poll_interval_ms: int = field(default=300000)
    fetch_max_bytes: int = field(default=52428800)  # 50MB
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for kafka-python consumer config."""
        return {
            "bootstrap_servers": self.bootstrap_servers.split(","),
            "group_id": self.group_id,
            "auto_offset_reset": self.auto_offset_reset,
            "enable_auto_commit": self.enable_auto_commit,
            "max_poll_records": self.max_poll_records,
            "session_timeout_ms": self.session_timeout_ms,
            "max_poll_interval_ms": self.max_poll_interval_ms,
            "fetch_max_bytes": self.fetch_max_bytes,
        }


@dataclass
class DatabaseConfig:
    """SQL database configuration."""
    
    driver: str = field(default="postgresql")
    host: str = field(default="localhost")
    port: int = field(default=5432)
    database: str = field(default="financial_db")
    username: str = field(default="postgres")
    password: str = field(default="")
    table_name: str = field(default="financial_positions")
    
    # Connection pool settings
    pool_size: int = field(default=5)
    max_overflow: int = field(default=10)
    pool_timeout: int = field(default=30)
    pool_recycle: int = field(default=3600)
    
    # Batch insert settings
    batch_size: int = field(default=1000)
    
    def get_connection_string(self) -> str:
        """Generate SQLAlchemy connection string."""
        if self.driver == "postgresql":
            return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.driver == "mssql":
            return f"mssql+pyodbc://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server"
        elif self.driver == "mysql":
            return f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        else:
            raise ValueError(f"Unsupported database driver: {self.driver}")


@dataclass
class SnapshotConfig:
    """Snapshot processing configuration."""
    
    interval_seconds: int = field(default=300)  # 5 minutes
    processing_timeout_seconds: int = field(default=240)  # 4 minutes
    max_retries: int = field(default=3)
    retry_backoff_seconds: int = field(default=5)
    
    # Snapshot settings
    enable_deduplication: bool = field(default=True)
    deduplication_keys: list = field(default_factory=lambda: ["position_id", "account_id"])


@dataclass
class LoggingConfig:
    """Logging configuration."""
    
    level: str = field(default="INFO")
    format: str = field(default="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    log_file: Optional[str] = field(default=None)
    enable_console: bool = field(default=True)


@dataclass
class Config:
    """Main configuration class for the financial snapshot library."""
    
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    snapshot: SnapshotConfig = field(default_factory=SnapshotConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    
    @classmethod
    def from_file(cls, config_path: str) -> "Config":
        """
        Load configuration from a JSON file.
        
        Args:
            config_path: Path to the configuration file
            
        Returns:
            Config instance with loaded settings
        """
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(path, "r") as f:
            config_dict = json.load(f)
        
        return cls.from_dict(config_dict)
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "Config":
        """
        Create Config from dictionary.
        
        Args:
            config_dict: Dictionary containing configuration
            
        Returns:
            Config instance
        """
        kafka_config = KafkaConfig(**config_dict.get("kafka", {}))
        database_config = DatabaseConfig(**config_dict.get("database", {}))
        snapshot_config = SnapshotConfig(**config_dict.get("snapshot", {}))
        logging_config = LoggingConfig(**config_dict.get("logging", {}))
        
        return cls(
            kafka=kafka_config,
            database=database_config,
            snapshot=snapshot_config,
            logging=logging_config,
        )
    
    @classmethod
    def from_env(cls) -> "Config":
        """
        Load configuration from environment variables.
        
        Environment variables should be prefixed with FS_ (Financial Snapshot).
        Example: FS_KAFKA_BOOTSTRAP_SERVERS, FS_DB_HOST, etc.
        
        Returns:
            Config instance with environment settings
        """
        kafka_config = KafkaConfig(
            bootstrap_servers=os.getenv("FS_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=os.getenv("FS_KAFKA_TOPIC", "financial_positions"),
            group_id=os.getenv("FS_KAFKA_GROUP_ID", "financial_snapshot_consumer"),
        )
        
        database_config = DatabaseConfig(
            driver=os.getenv("FS_DB_DRIVER", "postgresql"),
            host=os.getenv("FS_DB_HOST", "localhost"),
            port=int(os.getenv("FS_DB_PORT", "5432")),
            database=os.getenv("FS_DB_NAME", "financial_db"),
            username=os.getenv("FS_DB_USERNAME", "postgres"),
            password=os.getenv("FS_DB_PASSWORD", ""),
            table_name=os.getenv("FS_DB_TABLE", "financial_positions"),
            batch_size=int(os.getenv("FS_DB_BATCH_SIZE", "1000")),
        )
        
        snapshot_config = SnapshotConfig(
            interval_seconds=int(os.getenv("FS_SNAPSHOT_INTERVAL", "300")),
        )
        
        logging_config = LoggingConfig(
            level=os.getenv("FS_LOG_LEVEL", "INFO"),
            log_file=os.getenv("FS_LOG_FILE", None),
        )
        
        return cls(
            kafka=kafka_config,
            database=database_config,
            snapshot=snapshot_config,
            logging=logging_config,
        )
    
    def validate(self) -> None:
        """
        Validate configuration settings.
        
        Raises:
            ValueError: If configuration is invalid
        """
        # Validate Kafka config
        if not self.kafka.bootstrap_servers:
            raise ValueError("Kafka bootstrap_servers cannot be empty")
        if not self.kafka.topic:
            raise ValueError("Kafka topic cannot be empty")
        
        # Validate Database config
        if not self.database.host:
            raise ValueError("Database host cannot be empty")
        if not self.database.database:
            raise ValueError("Database name cannot be empty")
        if self.database.batch_size <= 0:
            raise ValueError("Database batch_size must be positive")
        
        # Validate Snapshot config
        if self.snapshot.interval_seconds <= 0:
            raise ValueError("Snapshot interval must be positive")
        if self.snapshot.processing_timeout_seconds <= 0:
            raise ValueError("Processing timeout must be positive")
        
        # Validate that timeout is less than interval
        if self.snapshot.processing_timeout_seconds >= self.snapshot.interval_seconds:
            raise ValueError("Processing timeout must be less than snapshot interval")
