# Quick Start Guide

## 5-Minute Setup

### 1. Install Dependencies

**Production (minimal dependencies)**:
```bash
pip install -r requirements-prod.txt
```

**Development (includes testing, notebooks, quality tools)**:
```bash
pip install -r requirements-dev.txt
```

**Or complete (all)**: 
```bash
pip install -r requirements.txt
```

### 2. Configure Credentials
```bash
cp config/example_credentials.yaml config/credentials.yaml
# Edit config/credentials.yaml with your Snowflake credentials
```

### 3. Test Connection
```bash
python -c "from config.connections import ConfigLoader; print(ConfigLoader.load_credentials('config/credentials.yaml'))"
```

### 4. Run ETL Pipeline
```bash
# Dry run (no execution)
python src/python/scripts/run_etl.py --dry-run

# Full pipeline
python src/python/scripts/run_etl.py --config config/dev.yaml
```

---

## Common Commands

### Development
```bash
# Run tests
pytest tests/ -v

# Format code
black src/

# Check code quality
pylint src/python/

# Run specific layer
python src/python/scripts/run_etl.py --layer gold --config config/dev.yaml
```

### Debugging
```bash
# Enable debug logging
python src/python/scripts/run_etl.py --log-level DEBUG

# View logs
tail -f logs/etl_full.log

# Dry run for validation
python src/python/scripts/run_etl.py --dry-run --log-level DEBUG
```

### Forecasting

```bash
# Run sales forecasting pipeline
python src/python/scripts/run_sales_forecast.py

# 180-day forecast
python src/python/scripts/run_sales_forecast.py --forecast-days 180

# Export forecast results
python src/python/scripts/run_sales_forecast.py --export forecast.csv

# Analyze results (Jupyter notebook)
jupyter notebook notebooks/sales_forecast_analysis.ipynb
```

### Production

```bash
# Run with production config
python src/python/scripts/run_etl.py --config config/prod.yaml --credentials config/credentials.yaml

# Schedule with cron (example: daily at 2 AM)
# 0 2 * * * cd /path/to/snowflake-study && python src/python/scripts/run_etl.py --config config/prod.yaml
```

---

## Project Structure

```
snowflake-study/
├── src/
│   ├── sql/
│   │   ├── bronze/        # Raw data loading SQL
│   │   ├── silver/        # Transformation SQL
│   │   └── gold/          # Analytics SQL
│   └── python/
│       ├── scripts/       # ETL orchestration
│       └── utils/         # Shared utilities
├── config/
│   ├── dev.yaml          # Development config
│   ├── staging.yaml      # Staging config
│   ├── prod.yaml         # Production config
│   └── connections.py    # Connection utilities
├── tests/
│   ├── unit/             # Unit tests
│   └── integration/      # Integration tests
├── docs/
│   ├── architecture.md   # Architecture guide
│   ├── data_dictionary.md # Data definitions
│   └── setup.md          # Setup guide
└── requirements.txt      # Python dependencies
```

---

## Key Files

| File | Purpose |
|------|---------|
| README.md | Project overview |
| docs/architecture.md | Architecture patterns and design |
| docs/setup.md | Deployment and operations guide |
| docs/data_dictionary.md | Table and column definitions |
| requirements.txt | Python dependencies |
| src/python/scripts/run_etl.py | Main ETL orchestrator |
| src/sql/bronze/ | Raw data templates |
| src/sql/silver/ | Transformation templates |
| src/sql/gold/ | Analytics templates |

---

## Configuration

### Environment Variables
```bash
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_ACCOUNT=xy12345.us-east-1
```

### Config File Format
```yaml
environment: development
debug: true

snowflake:
  warehouse: dev_compute_wh
  database: analytics_db
  schema: bronze_dev
  role: developer

pipeline:
  bronze:
    enabled: true
  silver:
    enabled: true
  gold:
    enabled: true
```

---

## Troubleshooting

### "Connection failed"
- Check credentials in `config/credentials.yaml`
- Verify Snowflake account is correct (include region)
- Test with Snowflake Worksheets first

### "SQL syntax error"
- Validate SQL in Snowflake UI before execution
- Check variable substitution (e.g., &{SCHEMA})
- Review SQL templates in appropriate layer

### "Data quality check failed"
- Review validation rules in `src/python/utils/data_validation.py`
- Check source data for anomalies
- Examine logs: `tail -f logs/etl_full.log`

### "Performance issues"
- Check query execution plans in Snowflake
- Review logs for execution times
- Monitor warehouse utilization
- Add indexes on filtering columns

---

## Next Steps

1. **Read the documentation**
   - Start with [docs/architecture.md](docs/architecture.md)
   - Review [docs/setup.md](docs/setup.md) for deployment

2. **Customize for your data**
   - Update SQL templates for your tables
   - Modify configurations in `config/`
   - Add data validation rules

3. **Set up CI/CD**
   - Configure GitHub Actions (see `.github/workflows/test.yml`)
   - Set up branch protection rules
   - Configure deployment pipelines

4. **Monitor and maintain**
   - Set up alerts and monitoring
   - Review logs regularly
   - Optimize slow queries
   - Update documentation

---

## Resources

- **Snowflake Docs**: https://docs.snowflake.com/
- **Medallion Architecture**: https://databricks.com/blog/2022/06/24/use-the-medallion-lakehouse-architecture.html
- **Data Warehousing**: https://en.wikipedia.org/wiki/Data_warehouse
- **Star Schema**: https://en.wikipedia.org/wiki/Star_schema

---

## Support

- Documentation: See `/docs` folder
- Issues: Create an issue in the repository
- Questions: Check setup guide and architecture docs

---

**Last Updated**: April 15, 2026
