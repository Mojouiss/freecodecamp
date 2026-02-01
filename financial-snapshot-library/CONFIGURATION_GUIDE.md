# Configuration Management Summary

## Overview

The Financial Snapshot Library now uses **environment-specific YAML configuration files** for managing settings across different deployment environments. MySQL/pymysql support has been removed, focusing on PostgreSQL and SQL Server with pyodbc.

## Environment-Specific Configuration Files

### Available Configurations

| File | Purpose | Database | Kafka | Logging |
|------|---------|----------|-------|---------|
| `config/dev.yaml` | Development | PostgreSQL (localhost) | localhost:9092 | DEBUG |
| `config/uat.yaml` | UAT Testing | PostgreSQL (UAT server) | UAT Kafka cluster | INFO |
| `config/prod.yaml` | Production | SQL Server (pyodbc) | Production Kafka cluster | INFO |
| `config/logging.yaml` | Logging config | N/A | N/A | Centralized config |

### Key Features

1. **Environment Separation**
   - Each environment has dedicated configuration
   - Prevents accidental production data access during development
   - Different log levels and output destinations per environment

2. **Security Best Practices**
   - UAT and production use environment variables for sensitive data: `${DB_PASSWORD}`
   - Development has explicit credentials (for local use only)
   - No hardcoded production credentials

3. **Database Support**
   - **PostgreSQL**: Primary database for dev/UAT (via psycopg2-binary)
   - **SQL Server**: Production database with pyodbc
   - **MySQL Removed**: pymysql dependency eliminated

4. **Logging Configuration**
   - Centralized logging.yaml with Python's logging.config format
   - Rotating file handlers (10MB per file, 5 backups)
   - Separate handlers for different log levels
   - JSON logging support (with pythonjsonlogger)

## Usage Examples

### Load Environment-Specific Config

```python
from financial_snapshot import Config

# Development
config = Config.from_file("config/dev.yaml")

# UAT
config = Config.from_file("config/uat.yaml")

# Production
config = Config.from_file("config/prod.yaml")
```

### Load with Environment Variables

```bash
# Set environment variables for sensitive data
export DB_USERNAME="prod_user"
export DB_PASSWORD="secure_password"

# Load config (environment variables will replace ${VAR} placeholders)
config = Config.from_file("config/prod.yaml")
```

### Configure Logging from YAML

```python
import logging.config
import yaml

with open("config/logging.yaml") as f:
    log_config = yaml.safe_load(f)
    logging.config.dictConfig(log_config)

logger = logging.getLogger("financial_snapshot")
logger.info("Application started")
```

## Configuration Differences by Environment

### Development (dev.yaml)

```yaml
# Optimized for local development
kafka:
  bootstrap_servers: localhost:9092
  topic: financial_positions_dev

database:
  driver: postgresql
  host: localhost
  username: postgres
  password: dev_password  # Explicit for local dev

logging:
  level: DEBUG  # Verbose logging
  enable_console: true
```

### UAT (uat.yaml)

```yaml
# Testing environment
kafka:
  bootstrap_servers: kafka-uat.example.com:9092
  topic: financial_positions_uat

database:
  driver: postgresql
  host: db-uat.example.com
  password: ${DB_PASSWORD}  # From environment

logging:
  level: INFO
  enable_console: true
```

### Production (prod.yaml)

```yaml
# Production-grade configuration
kafka:
  # High availability with multiple brokers
  bootstrap_servers: kafka-prod-1:9092,kafka-prod-2:9092,kafka-prod-3:9092
  topic: financial_positions

database:
  driver: mssql  # SQL Server with pyodbc
  host: sqlserver-prod.example.com
  port: 1433
  username: ${DB_USERNAME}
  password: ${DB_PASSWORD}
  pool_size: 10  # Larger pool for production
  max_overflow: 20

snapshot:
  max_retries: 5  # More retries in production
  retry_backoff_seconds: 10

logging:
  level: INFO
  log_file: /var/log/financial-snapshot/snapshot.log
  enable_console: false  # No console in production
```

## Migration from JSON to YAML

The library supports both JSON and YAML formats. Migration is straightforward:

### Before (config.json)

```json
{
  "kafka": {
    "bootstrap_servers": "localhost:9092",
    "topic": "financial_positions"
  },
  "database": {
    "driver": "postgresql",
    "host": "localhost"
  }
}
```

### After (config.yaml)

```yaml
kafka:
  bootstrap_servers: localhost:9092
  topic: financial_positions

database:
  driver: postgresql
  host: localhost
```

Both formats work with `Config.from_file()` - the library auto-detects by extension.

## Logging Configuration Details

The `logging.yaml` file provides:

1. **Multiple Formatters**
   - `standard`: Simple format for console output
   - `detailed`: Full context with process/thread IDs
   - `json`: Structured logging for log aggregation tools

2. **Multiple Handlers**
   - `console`: stdout for interactive debugging
   - `file`: Rotating file handler (10MB, 5 backups)
   - `error_file`: Separate file for ERROR+ level logs

3. **Logger Hierarchy**
   - `financial_snapshot`: Root logger for the library
   - `financial_snapshot.core`: Config and snapshot management
   - `financial_snapshot.messaging`: Kafka consumer
   - `financial_snapshot.storage`: Database writer
   - `confluent_kafka`: Kafka library (WARNING level)
   - `sqlalchemy`: Database library (WARNING level)

## Removed Dependencies

### pymysql Removed

**Reason**: Focus on enterprise databases (PostgreSQL and SQL Server)

**Impact**:
- MySQL support removed from `DatabaseConfig.get_connection_string()`
- `driver="mysql"` no longer accepted
- Tests updated to remove MySQL references
- Reduced dependency footprint

**Alternatives**:
- Use PostgreSQL for dev/UAT
- Use SQL Server with pyodbc for production
- If MySQL required, can use mysql-connector-python separately

## Benefits of This Approach

1. **Clear Environment Boundaries**
   - No confusion between dev/uat/prod settings
   - Easy to switch environments

2. **Security**
   - Environment variables for production secrets
   - No credentials in version control

3. **Maintainability**
   - YAML is more readable than JSON
   - Comments supported in YAML
   - Hierarchical structure is clearer

4. **Flexibility**
   - Different settings per environment
   - Easy to add new environments (e.g., staging.yaml)

5. **Best Practices**
   - Industry-standard approach (Kubernetes, Docker Compose, etc.)
   - Aligns with 12-factor app methodology
   - Easy integration with CI/CD pipelines

## CI/CD Integration

### Example: Kubernetes ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: financial-snapshot-config
data:
  config.yaml: |
    kafka:
      bootstrap_servers: kafka-service:9092
      topic: financial_positions
    database:
      driver: mssql
      host: sqlserver-service
      username: ${DB_USERNAME}
      password: ${DB_PASSWORD}
```

### Example: Docker Compose

```yaml
version: '3.8'
services:
  financial-snapshot:
    image: financial-snapshot:latest
    volumes:
      - ./config/prod.yaml:/app/config/config.yaml
    environment:
      - DB_USERNAME=prod_user
      - DB_PASSWORD=${DB_PASSWORD}
```

## Testing

All 36 unit tests pass with the new configuration system:

```bash
$ pytest tests/unit/ -v
======================== 36 passed, 1 warning in 0.70s =========================
```

Configuration tests validate:
- YAML file loading
- Environment variable substitution
- Pydantic validation
- Database connection string generation (PostgreSQL and SQL Server only)

## Summary

The refactored configuration system provides:

✅ **Environment-specific YAML files** (dev, uat, prod, logging)
✅ **Security best practices** (environment variables for secrets)
✅ **Simplified database support** (PostgreSQL and SQL Server with pyodbc)
✅ **Centralized logging configuration** (logging.yaml)
✅ **Backward compatibility** (JSON still supported)
✅ **All tests passing** (36 unit tests)

The library is now production-ready with enterprise-grade configuration management!
