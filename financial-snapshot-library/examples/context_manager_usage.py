"""
Example using context managers for automatic resource management.

This example demonstrates:
1. Using context managers for clean resource management
2. Automatic connection and disconnection
3. Exception handling
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from financial_snapshot import SnapshotManager, Config


def main():
    """Run snapshot with context manager."""
    print("=" * 60)
    print("Financial Snapshot Library - Context Manager Example")
    print("=" * 60)
    
    # Load configuration
    config_path = Path(__file__).parent.parent / "config" / "config.json"
    
    try:
        config = Config.from_file(str(config_path))
        print(f"✓ Configuration loaded from {config_path}")
    except FileNotFoundError:
        print(f"✗ Configuration file not found: {config_path}")
        print("Using default configuration from environment...")
        config = Config.from_env()
    
    config.validate()
    print("✓ Configuration validated\n")
    
    # Use context manager for automatic resource management
    print("Starting snapshot manager (context manager)...")
    print("Manager will run for 60 seconds, then stop automatically\n")
    
    try:
        # The 'with' statement automatically calls start() on entry
        # and stop() on exit, even if an exception occurs
        with SnapshotManager(config) as manager:
            print("✓ Manager started")
            
            # Let it run for a bit
            import time
            for i in range(6):  # 60 seconds total
                time.sleep(10)
                
                # Display metrics
                metrics = manager.get_metrics()
                print(f"  [{i*10}s] Snapshots: {metrics['total_snapshots']}, "
                      f"Records: {metrics['total_records_written']}")
        
        # At this point, manager.stop() has been called automatically
        print("\n✓ Manager stopped automatically")
    
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return 1
    
    print("\n" + "=" * 60)
    print("Example completed successfully")
    print("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
