"""
Unit tests for configuration module.
"""

import unittest
import json
import tempfile
import os
from pathlib import Path

from financial_snapshot.core.config import (
    Config,
    KafkaConfig,
    DatabaseConfig,
    SnapshotConfig,
    LoggingConfig,
)


class TestKafkaConfig(unittest.TestCase):
    """Test KafkaConfig class."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = KafkaConfig()
        
        self.assertEqual(config.bootstrap_servers, "localhost:9092")
        self.assertEqual(config.topic, "financial_positions")
        self.assertEqual(config.group_id, "financial_snapshot_consumer")
        self.assertEqual(config.max_poll_records, 500)
        self.assertFalse(config.enable_auto_commit)
    
    def test_to_dict(self):
        """Test conversion to dictionary."""
        config = KafkaConfig(
            bootstrap_servers="kafka1:9092,kafka2:9092",
            topic="test_topic",
        )
        
        config_dict = config.to_dict()
        
        self.assertEqual(config_dict["bootstrap_servers"], ["kafka1:9092", "kafka2:9092"])
        self.assertEqual(config_dict["group_id"], "financial_snapshot_consumer")
        self.assertIn("max_poll_records", config_dict)
    
    def test_custom_values(self):
        """Test custom configuration values."""
        config = KafkaConfig(
            bootstrap_servers="custom:9092",
            topic="custom_topic",
            max_poll_records=1000,
        )
        
        self.assertEqual(config.bootstrap_servers, "custom:9092")
        self.assertEqual(config.topic, "custom_topic")
        self.assertEqual(config.max_poll_records, 1000)


class TestDatabaseConfig(unittest.TestCase):
    """Test DatabaseConfig class."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = DatabaseConfig()
        
        self.assertEqual(config.driver, "postgresql")
        self.assertEqual(config.host, "localhost")
        self.assertEqual(config.port, 5432)
        self.assertEqual(config.database, "financial_db")
        self.assertEqual(config.batch_size, 1000)
    
    def test_postgresql_connection_string(self):
        """Test PostgreSQL connection string generation."""
        config = DatabaseConfig(
            driver="postgresql",
            host="db.example.com",
            port=5432,
            database="mydb",
            username="user",
            password="pass",
        )
        
        conn_str = config.get_connection_string()
        
        self.assertIn("postgresql://", conn_str)
        self.assertIn("user:pass", conn_str)
        self.assertIn("db.example.com:5432", conn_str)
        self.assertIn("mydb", conn_str)
    
    def test_mysql_connection_string(self):
        """Test MySQL connection string generation."""
        config = DatabaseConfig(
            driver="mysql",
            host="mysql.example.com",
            database="mydb",
        )
        
        conn_str = config.get_connection_string()
        
        self.assertIn("mysql+pymysql://", conn_str)
    
    def test_mssql_connection_string(self):
        """Test MSSQL connection string generation."""
        config = DatabaseConfig(
            driver="mssql",
            host="sqlserver.example.com",
            database="mydb",
        )
        
        conn_str = config.get_connection_string()
        
        self.assertIn("mssql+pyodbc://", conn_str)
    
    def test_invalid_driver(self):
        """Test invalid driver raises error."""
        config = DatabaseConfig(driver="invalid")
        
        with self.assertRaises(ValueError) as context:
            config.get_connection_string()
        
        self.assertIn("Unsupported database driver", str(context.exception))


class TestSnapshotConfig(unittest.TestCase):
    """Test SnapshotConfig class."""
    
    def test_default_values(self):
        """Test default configuration values."""
        config = SnapshotConfig()
        
        self.assertEqual(config.interval_seconds, 300)
        self.assertEqual(config.max_retries, 3)
        self.assertTrue(config.enable_deduplication)
        self.assertEqual(config.deduplication_keys, ["position_id", "account_id"])
    
    def test_custom_values(self):
        """Test custom configuration values."""
        config = SnapshotConfig(
            interval_seconds=600,
            max_retries=5,
            enable_deduplication=False,
        )
        
        self.assertEqual(config.interval_seconds, 600)
        self.assertEqual(config.max_retries, 5)
        self.assertFalse(config.enable_deduplication)


class TestConfig(unittest.TestCase):
    """Test main Config class."""
    
    def test_default_initialization(self):
        """Test default configuration initialization."""
        config = Config()
        
        self.assertIsInstance(config.kafka, KafkaConfig)
        self.assertIsInstance(config.database, DatabaseConfig)
        self.assertIsInstance(config.snapshot, SnapshotConfig)
        self.assertIsInstance(config.logging, LoggingConfig)
    
    def test_from_dict(self):
        """Test configuration from dictionary."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "kafka:9092",
                "topic": "test_topic",
            },
            "database": {
                "host": "db.example.com",
                "database": "test_db",
            },
            "snapshot": {
                "interval_seconds": 600,
            },
        }
        
        config = Config.from_dict(config_dict)
        
        self.assertEqual(config.kafka.bootstrap_servers, "kafka:9092")
        self.assertEqual(config.kafka.topic, "test_topic")
        self.assertEqual(config.database.host, "db.example.com")
        self.assertEqual(config.snapshot.interval_seconds, 600)
    
    def test_from_file(self):
        """Test configuration from JSON file."""
        config_dict = {
            "kafka": {
                "bootstrap_servers": "kafka:9092",
                "topic": "file_topic",
            },
            "database": {
                "host": "filedb.example.com",
            },
        }
        
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_dict, f)
            temp_file = f.name
        
        try:
            config = Config.from_file(temp_file)
            
            self.assertEqual(config.kafka.bootstrap_servers, "kafka:9092")
            self.assertEqual(config.kafka.topic, "file_topic")
            self.assertEqual(config.database.host, "filedb.example.com")
        finally:
            os.unlink(temp_file)
    
    def test_from_file_not_found(self):
        """Test error when config file doesn't exist."""
        with self.assertRaises(FileNotFoundError):
            Config.from_file("/nonexistent/config.json")
    
    def test_from_env(self):
        """Test configuration from environment variables."""
        # Set environment variables
        os.environ["FS_KAFKA_BOOTSTRAP_SERVERS"] = "env-kafka:9092"
        os.environ["FS_KAFKA_TOPIC"] = "env_topic"
        os.environ["FS_DB_HOST"] = "env-db.example.com"
        os.environ["FS_DB_PORT"] = "3306"
        os.environ["FS_SNAPSHOT_INTERVAL"] = "600"
        
        try:
            config = Config.from_env()
            
            self.assertEqual(config.kafka.bootstrap_servers, "env-kafka:9092")
            self.assertEqual(config.kafka.topic, "env_topic")
            self.assertEqual(config.database.host, "env-db.example.com")
            self.assertEqual(config.database.port, 3306)
            self.assertEqual(config.snapshot.interval_seconds, 600)
        finally:
            # Clean up environment variables
            for key in ["FS_KAFKA_BOOTSTRAP_SERVERS", "FS_KAFKA_TOPIC", "FS_DB_HOST", "FS_DB_PORT", "FS_SNAPSHOT_INTERVAL"]:
                os.environ.pop(key, None)
    
    def test_validate_success(self):
        """Test validation with valid configuration."""
        config = Config()
        
        # Should not raise any exception
        config.validate()
    
    def test_validate_empty_bootstrap_servers(self):
        """Test validation fails with empty bootstrap servers."""
        config = Config()
        config.kafka.bootstrap_servers = ""
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        
        self.assertIn("bootstrap_servers", str(context.exception))
    
    def test_validate_empty_topic(self):
        """Test validation fails with empty topic."""
        config = Config()
        config.kafka.topic = ""
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        
        self.assertIn("topic", str(context.exception))
    
    def test_validate_invalid_batch_size(self):
        """Test validation fails with invalid batch size."""
        config = Config()
        config.database.batch_size = -1
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        
        self.assertIn("batch_size", str(context.exception))
    
    def test_validate_invalid_interval(self):
        """Test validation fails with invalid interval."""
        config = Config()
        config.snapshot.interval_seconds = 0
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        
        self.assertIn("interval", str(context.exception))
    
    def test_validate_timeout_exceeds_interval(self):
        """Test validation fails when timeout exceeds interval."""
        config = Config()
        config.snapshot.interval_seconds = 100
        config.snapshot.processing_timeout_seconds = 200
        
        with self.assertRaises(ValueError) as context:
            config.validate()
        
        self.assertIn("timeout", str(context.exception).lower())


if __name__ == "__main__":
    unittest.main()
