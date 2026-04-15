# Setup and Deployment Guide

## Prerequisites

- Python 3.9 or higher
- Snowflake account and credentials
- Git for version control
- pip for Python package management

## Development Environment Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd snowflake-study
```

### 2. Create Virtual Environment

```bash
# On Windows
python -m venv venv
venv\Scripts\activate

# On macOS/Linux
python -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Snowflake Connection

```bash
# Copy example credentials
cp config/example_credentials.yaml config/credentials.yaml

# Edit with your credentials
# Use your favorite editor to update:
# - user
# - password
# - account
# - warehouse
# - database
# - schema
```

**Example credentials.yaml:**
```yaml
snowflake:
  user: your_snowflake_username
  password: your_snowflake_password
  account: xy12345.us-east-1  # Include region ID
  warehouse: compute_wh
  database: analytics_db
  schema: bronze
  role: sysadmin
```

### 5. Verify Connection

```bash
python -c "from config.connections import ConfigLoader; ConfigLoader.load_credentials('config/credentials.yaml')"
```

## Running the Pipeline

### Execute Full Pipeline

```bash
python src/python/scripts/run_etl.py --config config/dev.yaml --credentials config/credentials.yaml
```

### Execute Specific Layer

```bash
# Bronze layer only
python src/python/scripts/run_etl.py --config config/dev.yaml --credentials config/credentials.yaml --layer bronze

# Silver layer only
python src/python/scripts/run_etl.py --config config/dev.yaml --layer silver

# Gold layer only
python src/python/scripts/run_etl.py --config config/dev.yaml --layer gold
```

### Dry Run (Validation Only)

```bash
python src/python/scripts/run_etl.py --config config/dev.yaml --dry-run
```

### Additional Options

```bash
python src/python/scripts/run_etl.py --help

# Change log level
python src/python/scripts/run_etl.py --log-level DEBUG

# Use staging config
python src/python/scripts/run_etl.py --config config/staging.yaml

# Use production config
python src/python/scripts/run_etl.py --config config/prod.yaml
```

## Testing

### Run All Tests

```bash
pytest tests/ -v
```

### Run Only Unit Tests

```bash
pytest tests/unit/ -v
```

### Run Only Integration Tests

```bash
pytest tests/integration/ -v
```

### Generate Coverage Report

```bash
pytest tests/ --cov=src/python --cov-report=html
# Open htmlcov/index.html in browser
```

### Run Specific Test File

```bash
pytest tests/unit/test_data_validation.py -v
```

### Run with Detailed Output

```bash
pytest tests/ -v -s
```

## Code Quality

### Format Code with Black

```bash
black src/
```

### Check Code with Pylint

```bash
pylint src/python/
```

### Check Imports with isort

```bash
isort src/
```

### Run All Checks

```bash
black src/ --check
pylint src/python/
flake8 src/
```

## SQL Development

### Template SQL Files

SQL templates are provided for each layer:
- `src/sql/bronze/00_template_raw_load.sql`
- `src/sql/silver/00_template_cleaning_standardization.sql`
- `src/sql/gold/00_template_analytics_tables.sql`

### Adding New SQL Scripts

1. Create SQL file in appropriate layer directory:
   ```bash
   src/sql/bronze/01_load_customers.sql
   src/sql/silver/02_customer_dedup.sql
   src/sql/gold/03_customer_facts.sql
   ```

2. Use Snowflake Worksheets or SnowSQL to test locally

3. Execute via pipeline:
   ```bash
   python src/python/scripts/run_etl.py --layer bronze
   ```

## Environment-Specific Configuration

### Development Environment

- Located in: `config/dev.yaml`
- Purpose: Local development and testing
- Features:
  - Debug logging enabled
  - Smaller warehouse
  - Data quality checks enabled
  - No retention limits

```bash
python src/python/scripts/run_etl.py --config config/dev.yaml
```

### Staging Environment

- Located in: `config/staging.yaml`
- Purpose: Pre-production testing
- Features:
  - Production-like configuration
  - Medium warehouse size
  - Full validation enabled
  - Intermediate retention

```bash
python src/python/scripts/run_etl.py --config config/staging.yaml
```

### Production Environment

- Located in: `config/prod.yaml`
- Purpose: Production data pipeline
- Features:
  - Warning-level logging only
  - Large warehouse with auto-scaling
  - Monitoring and alerting enabled
  - Extended data retention

```bash
python src/python/scripts/run_etl.py --config config/prod.yaml
```

## Deployment Checklist

### Pre-Deployment
- [ ] All tests pass: `pytest tests/ -v`
- [ ] Code quality checks pass: `black --check src/`, `pylint src/`
- [ ] SQL scripts validated in Snowflake
- [ ] Configuration files reviewed
- [ ] Credentials configured securely

### Deployment Steps
1. Merge feature branch to main
2. Tag release version
3. Update CHANGELOG.md
4. Document any breaking changes
5. Notify stakeholders

### Post-Deployment
- [ ] Monitor pipeline execution
- [ ] Check data quality metrics
- [ ] Verify business metrics are correct
- [ ] Document any issues
- [ ] Plan rollback if needed

## Troubleshooting

### Connection Issues

**Error**: `snowflake.connector.errors.DatabaseError: 250001`

**Solution**:
- Verify Snowflake account name is correct
- Check user has appropriate role
- Verify password is correct
- Check network connectivity

### SQL Execution Errors

**Error**: `Syntax Error in SQL statement`

**Solution**:
- Check SQL file syntax in Snowflake Worksheets first
- Verify variable substitution (e.g., &{SCHEMA})
- Check table and column names are correct

### Performance Issues

**Solution**:
- Check query execution plans in Snowflake
- Review table statistics
- Add indexes on frequently filtered columns
- Use cluster keys for large tables
- Monitor warehouse utilization

### Data Quality Issues

**Error**: Quality checks failing

**Solution**:
- Review data validation rules in `src/python/utils/data_validation.py`
- Check source data for anomalies
- Update quality thresholds if appropriate
- Investigate upstream issues

## Monitoring & Alerting

### View Pipeline Logs

```bash
# Latest run
tail logs/etl_full.log

# Specific layer
tail logs/etl_bronze.log

# Follow live (update as written)
tail -f logs/etl_full.log
```

### Query Snowflake Query History

```sql
SELECT 
    query_id,
    query_text,
    execution_time,
    rows_produced,
    compilation_time,
    start_time
FROM snowflake.account_usage.query_history
WHERE database_name = 'ANALYTICS_DB'
ORDER BY start_time DESC
LIMIT 10;
```

## Maintenance Tasks

### Weekly
- [ ] Review pipeline logs for errors
- [ ] Check data quality metrics
- [ ] Monitor warehouse utilization and costs

### Monthly
- [ ] Review and optimize slow queries
- [ ] Update dependencies (pip check)
- [ ] Archive old logs and data

### Quarterly
- [ ] Full regression testing
- [ ] Performance optimization review
- [ ] Security audit and access review

## Support & Documentation

- Architecture documentation: [docs/architecture.md](architecture.md)
- Data dictionary: [docs/data_dictionary.md](data_dictionary.md)
- Main README: [README.md](../README.md)

## Version History

- v1.0.0 (2026-04-15): Initial setup documentation
