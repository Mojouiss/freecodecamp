# Production Deployment Guide

## Prerequisites

### System Requirements
- Python 3.8 or higher
- Apache Kafka cluster
- SQL Database (PostgreSQL, MySQL, or SQL Server)
- Minimum 512MB RAM
- Network access to Kafka and database

### Required Services
- Kafka broker(s) accessible on network
- Database server with created database
- (Optional) Log aggregation system

## Installation

### 1. Install the Library

```bash
# Clone or copy the library
cd financial-snapshot-library

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install the library
pip install -e .
```

### 2. Verify Installation

```bash
# Run tests to verify
python -m pytest tests/ -v

# Should see: 54 passed
```

## Configuration

### Environment Variables (Recommended for Production)

Create a `.env` file or export these variables:

```bash
# Kafka Configuration
export FS_KAFKA_BOOTSTRAP_SERVERS="kafka1.prod.example.com:9092,kafka2.prod.example.com:9092"
export FS_KAFKA_TOPIC="financial_positions"
export FS_KAFKA_GROUP_ID="financial_snapshot_prod"

# Database Configuration
export FS_DB_DRIVER="postgresql"
export FS_DB_HOST="postgres.prod.example.com"
export FS_DB_PORT="5432"
export FS_DB_NAME="financial_db"
export FS_DB_USERNAME="snapshot_user"
export FS_DB_PASSWORD="secure_password_here"
export FS_DB_TABLE="financial_positions"
export FS_DB_BATCH_SIZE="1000"

# Snapshot Configuration
export FS_SNAPSHOT_INTERVAL="300"  # 5 minutes

# Logging Configuration
export FS_LOG_LEVEL="INFO"
export FS_LOG_FILE="/var/log/financial-snapshot/app.log"
```

### Configuration File (Alternative)

Create `config/production.json`:

```json
{
  "kafka": {
    "bootstrap_servers": "kafka1.prod.example.com:9092,kafka2.prod.example.com:9092",
    "topic": "financial_positions",
    "group_id": "financial_snapshot_prod",
    "max_poll_records": 500
  },
  "database": {
    "driver": "postgresql",
    "host": "postgres.prod.example.com",
    "port": 5432,
    "database": "financial_db",
    "username": "snapshot_user",
    "password": "secure_password_here",
    "table_name": "financial_positions",
    "batch_size": 1000,
    "pool_size": 5
  },
  "snapshot": {
    "interval_seconds": 300,
    "processing_timeout_seconds": 240,
    "max_retries": 3,
    "enable_deduplication": true
  },
  "logging": {
    "level": "INFO",
    "log_file": "/var/log/financial-snapshot/app.log"
  }
}
```

## Database Setup

### PostgreSQL

```sql
-- Create database
CREATE DATABASE financial_db;

-- Create user
CREATE USER snapshot_user WITH ENCRYPTED PASSWORD 'secure_password_here';

-- Grant permissions
GRANT CONNECT ON DATABASE financial_db TO snapshot_user;
GRANT CREATE ON DATABASE financial_db TO snapshot_user;

-- Connect to the database
\c financial_db

-- Grant schema permissions
GRANT USAGE, CREATE ON SCHEMA public TO snapshot_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO snapshot_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO snapshot_user;

-- The library will automatically create the table on first run
```

### MySQL

```sql
-- Create database
CREATE DATABASE financial_db;

-- Create user
CREATE USER 'snapshot_user'@'%' IDENTIFIED BY 'secure_password_here';

-- Grant permissions
GRANT ALL PRIVILEGES ON financial_db.* TO 'snapshot_user'@'%';
FLUSH PRIVILEGES;

-- The library will automatically create the table on first run
```

### SQL Server

```sql
-- Create database
CREATE DATABASE financial_db;
GO

-- Create login and user
CREATE LOGIN snapshot_user WITH PASSWORD = 'secure_password_here';
USE financial_db;
CREATE USER snapshot_user FOR LOGIN snapshot_user;

-- Grant permissions
GRANT CREATE TABLE TO snapshot_user;
GRANT INSERT, SELECT ON SCHEMA::dbo TO snapshot_user;
GO

-- The library will automatically create the table on first run
```

## Running in Production

### Option 1: Python Script

Create `run_production.py`:

```python
#!/usr/bin/env python
"""Production runner for financial snapshot library."""

import sys
import signal
from financial_snapshot import SnapshotManager, Config

def signal_handler(sig, frame):
    """Handle shutdown signals gracefully."""
    print("Shutting down gracefully...")
    if 'manager' in globals():
        manager.stop()
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Load configuration from environment
config = Config.from_env()
config.validate()

# Create and start manager
manager = SnapshotManager(config)

print("Starting Financial Snapshot Manager...")
print(f"Kafka: {config.kafka.bootstrap_servers}")
print(f"Database: {config.database.host}/{config.database.database}")
print(f"Interval: {config.snapshot.interval_seconds} seconds")

try:
    manager.start()
    
    # Keep running
    import time
    while True:
        time.sleep(60)
        metrics = manager.get_metrics()
        print(f"Metrics: {metrics['total_snapshots']} snapshots, "
              f"{metrics['total_records_written']} records written")
        
except Exception as e:
    print(f"Error: {e}")
    manager.stop()
    sys.exit(1)
```

Run it:

```bash
chmod +x run_production.py
python run_production.py
```

### Option 2: Systemd Service

Create `/etc/systemd/system/financial-snapshot.service`:

```ini
[Unit]
Description=Financial Snapshot Service
After=network.target

[Service]
Type=simple
User=snapshot
Group=snapshot
WorkingDirectory=/opt/financial-snapshot
Environment="PATH=/opt/financial-snapshot/venv/bin"
EnvironmentFile=/etc/financial-snapshot/config.env
ExecStart=/opt/financial-snapshot/venv/bin/python run_production.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable financial-snapshot
sudo systemctl start financial-snapshot
sudo systemctl status financial-snapshot
```

View logs:

```bash
sudo journalctl -u financial-snapshot -f
```

### Option 3: Docker Container

Create `Dockerfile`:

```dockerfile
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY financial_snapshot/ ./financial_snapshot/
COPY examples/basic_usage.py ./run.py

# Create log directory
RUN mkdir -p /var/log/financial-snapshot

# Run as non-root user
RUN useradd -m -u 1000 snapshot && \
    chown -R snapshot:snapshot /app /var/log/financial-snapshot
USER snapshot

# Run the application
CMD ["python", "run.py"]
```

Build and run:

```bash
# Build image
docker build -t financial-snapshot:1.0.0 .

# Run container
docker run -d \
  --name financial-snapshot \
  --restart unless-stopped \
  -e FS_KAFKA_BOOTSTRAP_SERVERS="kafka:9092" \
  -e FS_DB_HOST="postgres" \
  -e FS_DB_PASSWORD="password" \
  -v /var/log/financial-snapshot:/var/log/financial-snapshot \
  financial-snapshot:1.0.0

# View logs
docker logs -f financial-snapshot
```

### Option 4: Docker Compose

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  financial-snapshot:
    build: .
    container_name: financial-snapshot
    restart: unless-stopped
    environment:
      FS_KAFKA_BOOTSTRAP_SERVERS: "kafka:9092"
      FS_KAFKA_TOPIC: "financial_positions"
      FS_DB_DRIVER: "postgresql"
      FS_DB_HOST: "postgres"
      FS_DB_PORT: "5432"
      FS_DB_NAME: "financial_db"
      FS_DB_USERNAME: "snapshot_user"
      FS_DB_PASSWORD: "${DB_PASSWORD}"
      FS_SNAPSHOT_INTERVAL: "300"
      FS_LOG_LEVEL: "INFO"
    volumes:
      - ./logs:/var/log/financial-snapshot
    depends_on:
      - kafka
      - postgres
    networks:
      - snapshot-net

  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_DB: financial_db
      POSTGRES_USER: snapshot_user
      POSTGRES_PASSWORD: "${DB_PASSWORD}"
    volumes:
      - postgres-data:/var/lib/postgresql/data
    networks:
      - snapshot-net

  kafka:
    image: confluentinc/cp-kafka:latest
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    depends_on:
      - zookeeper
    networks:
      - snapshot-net

  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    networks:
      - snapshot-net

volumes:
  postgres-data:

networks:
  snapshot-net:
```

Run:

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f financial-snapshot
```

### Option 5: Kubernetes Deployment

Create `k8s-deployment.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: financial-snapshot-config
data:
  FS_KAFKA_TOPIC: "financial_positions"
  FS_DB_DRIVER: "postgresql"
  FS_DB_TABLE: "financial_positions"
  FS_SNAPSHOT_INTERVAL: "300"
  FS_LOG_LEVEL: "INFO"

---
apiVersion: v1
kind: Secret
metadata:
  name: financial-snapshot-secrets
type: Opaque
stringData:
  FS_DB_PASSWORD: "your-secure-password"
  FS_KAFKA_BOOTSTRAP_SERVERS: "kafka-cluster.kafka.svc.cluster.local:9092"
  FS_DB_HOST: "postgres.database.svc.cluster.local"

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: financial-snapshot
  labels:
    app: financial-snapshot
spec:
  replicas: 1
  selector:
    matchLabels:
      app: financial-snapshot
  template:
    metadata:
      labels:
        app: financial-snapshot
    spec:
      containers:
      - name: financial-snapshot
        image: financial-snapshot:1.0.0
        envFrom:
        - configMapRef:
            name: financial-snapshot-config
        - secretRef:
            name: financial-snapshot-secrets
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
        livenessProbe:
          exec:
            command:
            - python
            - -c
            - "import sys; sys.exit(0)"
          initialDelaySeconds: 30
          periodSeconds: 60
```

Deploy:

```bash
kubectl apply -f k8s-deployment.yaml
kubectl logs -f deployment/financial-snapshot
```

## Monitoring

### Metrics Collection

```python
# Add to your runner script
import time
from financial_snapshot import SnapshotManager, Config

config = Config.from_env()
manager = SnapshotManager(config)
manager.start()

# Periodically log metrics
while True:
    time.sleep(60)
    metrics = manager.get_metrics()
    
    # Log to monitoring system
    print(f"METRICS: snapshots={metrics['total_snapshots']} "
          f"records={metrics['total_records_written']} "
          f"errors={metrics['total_errors']}")
```

### Health Checks

```python
# Simple health check endpoint
from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/health')
def health():
    """Health check endpoint."""
    metrics = manager.get_metrics()
    
    # Consider unhealthy if too many errors
    is_healthy = metrics['total_errors'] < 10
    
    return jsonify({
        'status': 'healthy' if is_healthy else 'unhealthy',
        'metrics': metrics
    }), 200 if is_healthy else 503

# Run alongside your snapshot manager
```

## Troubleshooting

### Common Issues

#### Issue: Cannot connect to Kafka

```bash
# Check Kafka connectivity
telnet kafka-host 9092

# Verify topic exists
kafka-topics.sh --list --bootstrap-server kafka-host:9092

# Check consumer group
kafka-consumer-groups.sh --bootstrap-server kafka-host:9092 --group financial_snapshot_consumer --describe
```

#### Issue: Cannot connect to database

```bash
# Test database connection
psql -h db-host -U snapshot_user -d financial_db

# Check table exists
\dt financial_positions
```

#### Issue: Low throughput

```
Solution: Increase batch sizes in configuration
- Increase max_poll_records (Kafka)
- Increase batch_size (Database)
- Increase pool_size (Database)
```

## Performance Tuning

### For High Volume (>10,000 records/minute)

```json
{
  "kafka": {
    "max_poll_records": 1000,
    "fetch_max_bytes": 104857600
  },
  "database": {
    "batch_size": 2000,
    "pool_size": 10,
    "max_overflow": 20
  }
}
```

### For Low Latency (<1 second)

```json
{
  "kafka": {
    "max_poll_records": 100
  },
  "database": {
    "batch_size": 100,
    "pool_size": 3
  },
  "snapshot": {
    "interval_seconds": 60
  }
}
```

## Security Best Practices

1. **Never hardcode credentials** - Use environment variables or secrets management
2. **Use SSL/TLS** for Kafka and database connections
3. **Rotate credentials** regularly
4. **Run as non-root** user
5. **Limit database permissions** to only what's needed
6. **Encrypt logs** if they contain sensitive data
7. **Use network policies** to restrict access

## Backup and Recovery

### Database Backups

```bash
# PostgreSQL backup
pg_dump -h postgres.example.com -U snapshot_user financial_db > backup.sql

# Restore
psql -h postgres.example.com -U snapshot_user financial_db < backup.sql
```

### Kafka Offset Reset

```bash
# Reset to earliest offset
kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group financial_snapshot_consumer \
  --reset-offsets --to-earliest \
  --topic financial_positions \
  --execute
```

## Maintenance

### Log Rotation

Create `/etc/logrotate.d/financial-snapshot`:

```
/var/log/financial-snapshot/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0640 snapshot snapshot
}
```

### Database Maintenance

```sql
-- Regular vacuum (PostgreSQL)
VACUUM ANALYZE financial_positions;

-- Archive old data (>90 days)
CREATE TABLE financial_positions_archive AS 
SELECT * FROM financial_positions 
WHERE created_at < NOW() - INTERVAL '90 days';

DELETE FROM financial_positions 
WHERE created_at < NOW() - INTERVAL '90 days';
```

## Support

For production issues:
1. Check logs first: `/var/log/financial-snapshot/app.log`
2. Review metrics from the manager
3. Verify Kafka and database connectivity
4. Check system resources (CPU, memory, disk)
5. Consult the documentation in README.md and ARCHITECTURE.md

## License

BSD-3-Clause License - See LICENSE file for details.
