"""
Example of running a single snapshot capture.

This example shows how to:
1. Configure the library programmatically
2. Run a one-time snapshot
3. Display results
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from financial_snapshot import SnapshotManager, Config
from financial_snapshot.core.config import (
    KafkaConfig,
    DatabaseConfig,
    SnapshotConfig,
    LoggingConfig,
)


def main():
    """Run a single snapshot capture."""
    print("=" * 60)
    print("Financial Snapshot Library - One-Time Snapshot")
    print("=" * 60)
    
    # Create configuration programmatically
    kafka_config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        topic="financial_positions",
        group_id="financial_snapshot_consumer",
        max_poll_records=500,
    )
    
    database_config = DatabaseConfig(
        driver="postgresql",
        host="localhost",
        port=5432,
        database="financial_db",
        username="postgres",
        password="your_password",
        table_name="financial_positions",
        batch_size=1000,
    )
    
    snapshot_config = SnapshotConfig(
        interval_seconds=300,
        max_retries=3,
        enable_deduplication=True,
    )
    
    logging_config = LoggingConfig(
        level="INFO",
        enable_console=True,
    )
    
    config = Config(
        kafka=kafka_config,
        database=database_config,
        snapshot=snapshot_config,
        logging=logging_config,
    )
    
    print("✓ Configuration created programmatically")
    
    # Validate configuration
    try:
        config.validate()
        print("✓ Configuration validated successfully")
    except ValueError as e:
        print(f"✗ Configuration validation failed: {e}")
        return
    
    # Create snapshot manager
    manager = SnapshotManager(config)
    
    print("\n" + "=" * 60)
    print("Executing single snapshot capture...")
    print("=" * 60 + "\n")
    
    try:
        # Run a single snapshot
        result = manager.run_once()
        
        # Display results
        print("\n" + "=" * 60)
        print("Snapshot Results:")
        print("=" * 60)
        print(f"  Snapshot ID: {result['snapshot_id']}")
        print(f"  Success: {result['success']}")
        print(f"  Records Consumed: {result['records_consumed']}")
        print(f"  Records Valid: {result['records_valid']}")
        print(f"  Records Written: {result['records_written']}")
        print(f"  Records Invalid: {result['records_invalid']}")
        print(f"  Duration: {result['duration_seconds']:.2f} seconds")
        
        if result['records_written'] > 0:
            rps = result['records_written'] / result['duration_seconds']
            print(f"  Throughput: {rps:.2f} records/second")
        
        if result['error']:
            print(f"  Error: {result['error']}")
        
        print("=" * 60)
        
        # Disconnect cleanly
        manager.consumer.disconnect()
        manager.writer.disconnect()
        
        if result['success']:
            print("\n✓ Snapshot completed successfully")
        else:
            print("\n✗ Snapshot failed")
            return 1
    
    except Exception as e:
        print(f"\n✗ Error during snapshot: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
