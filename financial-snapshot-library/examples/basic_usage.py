"""
Basic example of using the Financial Snapshot Library.

This example demonstrates how to:
1. Load configuration from a file
2. Create a snapshot manager
3. Run continuous snapshots
"""

import sys
import time
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from financial_snapshot import SnapshotManager, Config


def main():
    """Run the financial snapshot manager."""
    print("=" * 60)
    print("Financial Snapshot Library - Basic Example")
    print("=" * 60)
    
    # Load configuration
    config_path = Path(__file__).parent.parent / "config" / "config.json"
    
    try:
        config = Config.from_file(str(config_path))
        print(f"✓ Configuration loaded from {config_path}")
    except FileNotFoundError:
        print(f"✗ Configuration file not found: {config_path}")
        print("Creating configuration from environment variables...")
        config = Config.from_env()
    
    # Validate configuration
    try:
        config.validate()
        print("✓ Configuration validated successfully")
    except ValueError as e:
        print(f"✗ Configuration validation failed: {e}")
        return
    
    print(f"\nSnapshot Configuration:")
    print(f"  - Interval: {config.snapshot.interval_seconds} seconds")
    print(f"  - Kafka Topic: {config.kafka.topic}")
    print(f"  - Database: {config.database.driver}://{config.database.host}/{config.database.database}")
    print(f"  - Deduplication: {config.snapshot.enable_deduplication}")
    
    # Create snapshot manager
    manager = SnapshotManager(config)
    
    print("\n" + "=" * 60)
    print("Starting snapshot manager...")
    print("Press Ctrl+C to stop")
    print("=" * 60 + "\n")
    
    try:
        # Start the manager
        manager.start()
        
        # Keep running and display metrics periodically
        while True:
            time.sleep(30)  # Display metrics every 30 seconds
            
            metrics = manager.get_metrics()
            print("\n" + "-" * 60)
            print("Current Metrics:")
            print(f"  Total Snapshots: {metrics['total_snapshots']}")
            print(f"  Records Processed: {metrics['total_records_processed']}")
            print(f"  Records Written: {metrics['total_records_written']}")
            print(f"  Invalid Records: {metrics['total_invalid_records']}")
            print(f"  Errors: {metrics['total_errors']}")
            
            if metrics['last_snapshot_time']:
                print(f"  Last Snapshot: {metrics['last_snapshot_time']}")
                print(f"  Last Duration: {metrics['last_snapshot_duration']:.2f}s")
            print("-" * 60)
    
    except KeyboardInterrupt:
        print("\n\nShutting down gracefully...")
    
    except Exception as e:
        print(f"\n✗ Error: {e}")
    
    finally:
        # Stop the manager
        manager.stop()
        print("✓ Snapshot manager stopped")
        
        # Display final metrics
        metrics = manager.get_metrics()
        print("\n" + "=" * 60)
        print("Final Metrics:")
        print(f"  Total Snapshots: {metrics['total_snapshots']}")
        print(f"  Total Records Processed: {metrics['total_records_processed']}")
        print(f"  Total Records Written: {metrics['total_records_written']}")
        print(f"  Total Invalid Records: {metrics['total_invalid_records']}")
        print(f"  Total Errors: {metrics['total_errors']}")
        print("=" * 60)


if __name__ == "__main__":
    main()
