"""
Configuration management for the financial snapshot library.

This module provides a robust configuration system with validation,
environment variable support, and sensible defaults using Pydantic.
"""

from typing import Dict, Any, Optional, List
from pathlib import Path
import json

from pydantic import BaseModel, Field, field_validator, ConfigDict
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaConfig(BaseModel):
    """Kafka consumer configuration for confluent-kafka."""
    
    model_config = ConfigDict(validate_assignment=True)
    
    bootstrap_servers: str = Field(default="localhost:9092", description="Comma-separated Kafka brokers")
    topic: str = Field(default="financial_positions", description="Kafka topic name")
    group_id: str = Field(default="financial_snapshot_consumer", description="Consumer group ID")
    auto_offset_reset: str = Field(default="earliest", description="Offset reset strategy")
    enable_auto_commit: bool = Field(default=False, description="Enable auto commit")
    max_poll_records: int = Field(default=500, ge=1, description="Max records per poll")
    session_timeout_ms: int = Field(default=30000, ge=1000, description="Session timeout")
    max_poll_interval_ms: int = Field(default=300000, ge=1000, description="Max poll interval")
    fetch_max_bytes: int = Field(default=52428800, ge=1024, description="Max fetch bytes")
    
    def to_confluent_config(self) -> Dict[str, Any]:
        """Convert to confluent-kafka consumer config."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": self.enable_auto_commit,
            "session.timeout.ms": self.session_timeout_ms,
            "max.poll.interval.ms": self.max_poll_interval_ms,
            "fetch.max.bytes": self.fetch_max_bytes,
        }


class DatabaseConfig(BaseModel):
    """SQL database configuration."""
    
    model_config = ConfigDict(validate_assignment=True)
    
    driver: str = Field(default="postgresql", description="Database driver")
    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=5432, ge=1, le=65535, description="Database port")
    database: str = Field(default="financial_db", min_length=1, description="Database name")
    username: str = Field(default="postgres", description="Database username")
    password: str = Field(default="", description="Database password")
    table_name: str = Field(default="financial_positions", min_length=1, description="Table name")
    
    # Connection pool settings
    pool_size: int = Field(default=5, ge=1, description="Connection pool size")
    max_overflow: int = Field(default=10, ge=0, description="Max overflow connections")
    pool_timeout: int = Field(default=30, ge=1, description="Pool timeout in seconds")
    pool_recycle: int = Field(default=3600, ge=60, description="Pool recycle time in seconds")
    
    # Batch insert settings
    batch_size: int = Field(default=1000, ge=1, description="Batch insert size")
    
    @field_validator("driver")
    @classmethod
    def validate_driver(cls, v: str) -> str:
        """Validate database driver."""
        allowed = ["postgresql", "mysql", "mssql"]
        if v not in allowed:
            raise ValueError(f"Driver must be one of {allowed}")
        return v
    
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


class SnapshotConfig(BaseModel):
    """Snapshot processing configuration."""
    
    model_config = ConfigDict(validate_assignment=True)
    
    interval_seconds: int = Field(default=300, ge=1, description="Snapshot interval in seconds")
    processing_timeout_seconds: int = Field(default=240, ge=1, description="Processing timeout")
    max_retries: int = Field(default=3, ge=0, description="Max retry attempts")
    retry_backoff_seconds: int = Field(default=5, ge=1, description="Retry backoff in seconds")
    
    # Snapshot settings
    enable_deduplication: bool = Field(default=True, description="Enable deduplication")
    deduplication_keys: List[str] = Field(
        default_factory=lambda: ["position_id", "account_id"],
        description="Deduplication key fields"
    )
    
    @field_validator("processing_timeout_seconds")
    @classmethod
    def validate_timeout(cls, v: int, info) -> int:
        """Validate timeout is less than interval."""
        # Note: In Pydantic v2, we can't access other fields during validation
        # This validation will be done in the Config.model_post_init
        return v


class LoggingConfig(BaseModel):
    """Logging configuration."""
    
    model_config = ConfigDict(validate_assignment=True)
    
    level: str = Field(default="INFO", description="Log level")
    format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format"
    )
    log_file: Optional[str] = Field(default=None, description="Log file path")
    enable_console: bool = Field(default=True, description="Enable console logging")
    
    @field_validator("level")
    @classmethod
    def validate_level(cls, v: str) -> str:
        """Validate log level."""
        allowed = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        v_upper = v.upper()
        if v_upper not in allowed:
            raise ValueError(f"Log level must be one of {allowed}")
        return v_upper


class Config(BaseSettings):
    """Main configuration class for the financial snapshot library."""
    
    model_config = SettingsConfigDict(
        env_prefix="FS_",
        env_nested_delimiter="__",
        case_sensitive=False,
        validate_assignment=True,
    )
    
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    snapshot: SnapshotConfig = Field(default_factory=SnapshotConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    
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
        
        return cls(**config_dict)
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "Config":
        """
        Create Config from dictionary.
        
        Args:
            config_dict: Dictionary containing configuration
            
        Returns:
            Config instance
        """
        return cls(**config_dict)
    
    @classmethod
    def from_env(cls) -> "Config":
        """
        Load configuration from environment variables.
        
        Environment variables should be prefixed with FS_ (Financial Snapshot).
        Examples:
            FS_KAFKA__BOOTSTRAP_SERVERS=kafka:9092
            FS_DATABASE__HOST=db.example.com
            FS_SNAPSHOT__INTERVAL_SECONDS=600
        
        Returns:
            Config instance with environment settings
        """
        return cls()
    
    def model_post_init(self, __context: Any) -> None:
        """Validate configuration after initialization."""
        self.validate()
    
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
        
        # Validate that timeout is less than interval
        if self.snapshot.processing_timeout_seconds >= self.snapshot.interval_seconds:
            raise ValueError("Processing timeout must be less than snapshot interval")
