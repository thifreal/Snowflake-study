# Snowflake Data Engineering Repository

A professional, production-ready Snowflake project following the **Medallion Architecture Pattern** (Bronze → Silver → Gold) and industry best practices.

## 🏗️ Project Architecture

### Medallion Pattern Layers

- **Bronze Layer**: Raw data ingestion from source systems. Minimal transformation, maintains original structure.
- **Silver Layer**: Cleaned, deduplicated, and lightly transformed data. Quality checks and standardization.
- **Gold Layer**: Business-ready analytical tables, aggregations, and insights. Optimized for reporting and analysis.

## 📁 Directory Structure

```
snowflake-study/
├── src/
│   ├── sql/
│   │   ├── bronze/          # Raw data load, schema definitions
│   │   ├── silver/          # Data cleaning, transformation, deduplication
│   │   └── gold/            # Aggregated views, reporting tables
│   ├── python/
│   │   ├── scripts/         # Data pipeline scripts, orchestration
│   │   └── utils/           # Reusable utilities, helpers
│   └── dbt/                 # dbt project (optional)
├── config/
│   ├── dev.yaml            # Development environment config
│   ├── staging.yaml        # Staging environment config
│   ├── prod.yaml           # Production environment config
│   └── connections.py      # Snowflake connection utilities
├── tests/
│   ├── unit/               # Unit tests for Python code
│   └── integration/        # Integration tests
├── docs/
│   ├── architecture.md     # Architecture documentation
│   ├── data_dictionary.md  # Data definitions and lineage
│   └── setup.md            # Setup and deployment guide
├── .github/
│   └── workflows/          # CI/CD pipeline definitions
├── requirements.txt        # Python dependencies
├── .gitignore             # Git ignore rules
└── README.md              # This file
```

## 🚀 Getting Started

### Prerequisites
- Python 3.9+
- Snowflake account and credentials
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd snowflake-study
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   
   Choose installation based on your use case:
   
   - **Production** (minimal): `pip install -r requirements-prod.txt`
   - **Development** (testing + notebooks): `pip install -r requirements-dev.txt`
   - **Complete** (all features): `pip install -r requirements.txt`
   
   📖 **See [INSTALLATION.md](INSTALLATION.md) for detailed guidance, troubleshooting, and platform-specific notes.**

4. **Configure Snowflake connection**
   ```bash
   # Create config/credentials.yaml with your Snowflake details
   cp config/example_credentials.yaml config/credentials.yaml
   # Edit config/credentials.yaml with your credentials
   ```

## 📊 Layer Responsibilities

### Bronze Layer
- Ingest raw data from source systems
- Minimal transformation
- Full historical tracking (created_date, modified_date)
- Example: `src/sql/bronze/*.sql`

### Silver Layer
- Data cleansing and quality checks
- Standardization and deduplication
- Schema validation
- Example: `src/sql/silver/*.sql`

### Gold Layer
- Business aggregate views
- Dimensional and fact tables
- Performance optimized
- Example: `src/sql/gold/*.sql`

## 🔄 Data Pipeline Execution

```bash
# Run full ETL pipeline
python src/python/scripts/run_etl.py --config config/prod.yaml

# Run specific layer
python src/python/scripts/run_etl.py --config config/prod.yaml --layer gold

# Dry run (validate without executing)
python src/python/scripts/run_etl.py --config config/prod.yaml --dry-run
```

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Run unit tests only
pytest tests/unit/ -v

# Run integration tests
pytest tests/integration/ -v

# Run with coverage
pytest tests/ --cov=src/python/ --cov-report=html
```

## 📝 SQL Conventions

- **Naming**: Snake_case for tables and columns
- **Comments**: Add docstrings to all SQL files
- **Transactions**: Use explicit BEGIN/COMMIT for data modifications
- **Performance**: Add indexes on frequently queried columns
- **Documentation**: Maintain data dictionary in `docs/data_dictionary.md`

## 🔐 Security Best Practices

- Never commit credentials or secrets
- Use environment variables or `.env` files
- Implement row-level security (RLS) where appropriate
- Regular access audits
- Enable query history and monitoring

## 📚 Documentation

Comprehensive documentation is available in:
- [Architecture](docs/architecture.md)
- [Data Dictionary](docs/data_dictionary.md)
- [Setup Guide](docs/setup.md)

## 📚 Example Projects

### Sales Forecasting Project
This repository includes a complete **end-to-end sales forecasting project** that demonstrates:
- Data generation with realistic patterns (trend, seasonality, noise)
- Complete medallion architecture implementation
- Snowflake ML time-series forecasting
- 90-day and 180-day revenue predictions with confidence intervals
- Analysis notebooks and interactive visualizations

**Get Started**:
```bash
# Run the sales forecasting pipeline
python src/python/scripts/run_sales_forecast.py --config config/dev.yaml

# Explore results in Jupyter notebook
jupyter notebook notebooks/sales_forecast_analysis.ipynb
```

**Learn More**: [Sales Forecast Project Guide](docs/SALES_FORECAST_PROJECT.md)

## 🤝 Contributing

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Commit changes: `git commit -m "Add feature"`
3. Push to branch: `git push origin feature/your-feature`
4. Create Pull Request

## 📋 Code Quality

- Python code must pass `pylint` and `black` formatters
- SQL files must follow naming conventions
- All code changes require tests
- Documentation must be kept up-to-date

## 🐛 Troubleshooting

### Connection Issues
- Verify Snowflake credentials in config files
- Check firewall and network connectivity
- Ensure correct account and region setup

### Performance Issues
- Check query execution plans in Snowflake
- Review table statistics and indexes
- Monitor warehouse size and scaling

## 📞 Support

For issues or questions:
1. Check existing documentation
2. Review error logs in `logs/` directory
3. Create an issue in the repository

## 📄 License

[Add your license information here]

## 📅 Version History

- v1.0.0 (2026-04-15): Initial repository setup with medallion architecture

---

**Last Updated**: April 15, 2026
