# Sales Forecasting Project

## 📊 Overview

This is an end-to-end sales revenue forecasting project that demonstrates the complete data pipeline using Snowflake ML's built-in forecasting capabilities. The pipeline follows the medallion architecture pattern to generate synthetic sales data, clean it, analyze trends, and create machine learning forecasts.

## 🎯 Project Objectives

- Generate realistic 2-year historical sales data with patterns (trend, seasonality, weekday effect)
- Implement complete medallion architecture (Bronze → Silver → Gold)
- Demonstrate data exploration and quality checks
- Train a Snowflake ML forecasting model on time-series data
- Generate 90-day and 180-day revenue forecasts with confidence intervals
- Create business-ready analytics and reporting views

## 📂 Project Structure

```
Snowflake-study/
├── src/sql/
│   ├── bronze/
│   │   └── 01_sales_raw_data_generator.sql      # Generate 2 years of sales data
│   ├── silver/
│   │   └── 01_sales_cleaning_features.sql       # Data cleaning & feature engineering
│   └── gold/
│       ├── 01_sales_analytics_facts.sql         # Analytics tables
│       └── 02_sales_forecasting_model.sql       # ML forecasting model
├── src/python/scripts/
│   ├── run_etl.py                              # General ETL orchestrator
│   └── run_sales_forecast.py                   # Sales forecast pipeline
├── docs/
│   └── SALES_FORECAST_README.md                # This file
└── config/
    ├── dev.yaml                                # Development config
    └── example_credentials.yaml                # Credentials template
```

## 🏗️ Data Pipeline Architecture

### Layer 1: Bronze (Raw Data)
**File**: `src/sql/bronze/01_sales_raw_data_generator.sql`

Creates mock daily sales data with realistic patterns:
- **Time Period**: 2023-01-01 to 2024-12-31 (730 days)
- **Patterns**:
  - Growth Trend: +0.15 per day (natural business growth)
  - Seasonality: Monthly variations (holidays peak, post-holiday dip)
  - Weekday Effect: Weekends lower than weekdays
  - Random Noise: ±15 variance for realism

**Tables Created**:
- `bronze_daily_sales`: Raw daily sales with order counts

### Layer 2: Silver (Processed Data)
**File**: `src/sql/silver/01_sales_cleaning_features.sql`

Transforms and enriches data for analytics:
- ✓ Deduplication and outlier detection
- ✓ Time-series features (year, month, quarter, day of week, week of year)
- ✓ Derived metrics (revenue per order, z-scores)
- ✓ Business flags (weekend, holiday season)
- ✓ Data quality validation

**Tables/Views Created**:
- `silver_daily_sales`: Cleaned daily sales with features
- `silver_vw_monthly_sales`: Monthly aggregations
- `silver_vw_quarterly_sales`: Quarterly summaries
- `silver_vw_day_analysis`: Weekday vs weekend patterns

### Layer 3: Gold (Analytics & Forecasting)
**Files**: 
- `src/sql/gold/01_sales_analytics_facts.sql`
- `src/sql/gold/02_sales_forecasting_model.sql`

Creates business-ready analytics and ML models:

**Analytics Tables**:
- `gold_fact_daily_sales`: Fact table with rolling aggregates
- `gold_agg_monthly_sales`: Pre-aggregated monthly metrics
- `gold_agg_quarterly_sales`: Pre-aggregated quarterly metrics

**ML Forecasting**:
- `gold_sales_forecast_model`: Trained time-series ML model
- `gold_forecast_revenue_90days`: 90-day predictions with 95% CI
- `gold_forecast_revenue_180days`: 180-day predictions with 95% CI
- `gold_vw_actuals_vs_forecast_90days`: Combined view for dashboards
- `gold_vw_actuals_vs_forecast_180days`: Long-term planning view

## 🚀 Quick Start

### 1. Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Configure credentials
cp config/example_credentials.yaml config/credentials.yaml
# Edit config/credentials.yaml with your Snowflake account details
```

### 2. Run Sales Forecast Pipeline

```bash
# Full pipeline execution
python src/python/scripts/run_sales_forecast.py --config config/dev.yaml

# Dry run (validation only)
python src/python/scripts/run_sales_forecast.py --dry-run

# 180-day forecast
python src/python/scripts/run_sales_forecast.py --forecast-days 180

# Export forecast to CSV
python src/python/scripts/run_sales_forecast.py --export forecast.csv
```

### 3. Alternative: Execute SQL Directly

```sql
-- Run in Snowflake Worksheet in order:

-- 1. Generate raw data (Bronze)
USE DATABASE <your_db>;
USE SCHEMA <your_schema>;
EXECUTE IMMEDIATE FROM 'src/sql/bronze/01_sales_raw_data_generator.sql';

-- 2. Clean and transform (Silver)
EXECUTE IMMEDIATE FROM 'src/sql/silver/01_sales_cleaning_features.sql';

-- 3. Create analytics (Gold)
EXECUTE IMMEDIATE FROM 'src/sql/gold/01_sales_analytics_facts.sql';

-- 4. Train ML model (Gold)
EXECUTE IMMEDIATE FROM 'src/sql/gold/02_sales_forecasting_model.sql';
```

## 📈 Data Flow

```
Generate Raw Data (730 days)
    ↓
BRONZE: bronze_daily_sales
    ├─ 730 rows of daily sales
    ├─ Realistic patterns (trend, seasonality, noise)
    └─ Metadata (_load_date, _source_system)
    
Clean & Transform
    ↓
SILVER: silver_daily_sales
    ├─ Deduplication
    ├─ Outlier detection (Z-score)
    ├─ Feature engineering (month, day_of_week, etc.)
    ├─ Revenue per order calculation
    └─ Views: monthly, quarterly, day analysis
    
Analyze & Forecast
    ↓
GOLD: Analytics & ML
    ├─ Fact tables: gold_fact_daily_sales
    ├─ Aggregates: monthly, quarterly
    ├─ Materialized views for performance
    ├─ ML Model: gold_sales_forecast_model
    └─ Forecasts: 90-day, 180-day predictions
```

## 🔍 Key Analyses

### Data Exploration
```sql
-- Summary statistics
SELECT 
    COUNT(*) AS total_days,
    MIN(sale_date) AS first_date,
    MAX(sale_date) AS last_date,
    ROUND(AVG(daily_revenue), 2) AS avg_revenue,
    ROUND(SUM(daily_revenue), 2) AS total_revenue
FROM silver_daily_sales;

-- Monthly breakdown
SELECT * FROM silver_vw_monthly_sales ORDER BY month_date;

-- Day-of-week patterns
SELECT * FROM silver_vw_day_analysis ORDER BY day_of_week;
```

### Forecast Inspection
```sql
-- Model performance metrics
CALL gold_sales_forecast_model!SHOW_EVALUATION_METRICS();

-- View near-term forecast
SELECT * FROM gold_vw_actuals_vs_forecast_90days
WHERE date >= CURRENT_DATE() - 7 
ORDER BY date;

-- Monthly forecast summary
SELECT * FROM gold_vw_forecast_monthly_summary;

-- Forecast confidence levels
SELECT 
    forecast_date,
    forecasted_revenue,
    ROUND((upper_bound_95pct - lower_bound_95pct) / forecasted_revenue * 100, 2) AS uncertainty_pct
FROM gold_forecast_revenue_90days
LIMIT 30;
```

## 📊 Snowflake ML Forecasting

### Model Quality Metrics

The forecasting model uses Snowflake's built-in time-series algorithms and evaluates performance with:

| Metric | Meaning | Target |
|--------|---------|--------|
| **MAPE** | Mean Absolute Percentage Error | < 10% |
| **MAE** | Mean Absolute Error (in dollars) | Lower is better |
| **MSE** | Mean Squared Error | Lower is better |
| **RMSE** | Root Mean Squared Error | Lower is better |
| **Coverage** | % of actuals in 95% CI | > 95% |

### Confidence Intervals

The forecast provides 95% confidence intervals:
- **FORECAST**: Point estimate (most likely value)
- **LOWER_BOUND**: 5th percentile (conservative estimate)
- **UPPER_BOUND**: 95th percentile (optimistic estimate)

Use these for:
- **Conservative Planning**: Lower bound for budgeting
- **Optimistic Planning**: Upper bound for growth scenarios
- **Uncertainty Quantification**: Width indicates forecast reliability

## 💡 Use Cases

### 1. Revenue Forecasting
Predict future daily, weekly, and monthly revenues for budgeting and financial planning.

### 2. Capacity Planning
Use forecast confidence intervals to plan inventory, staffing, and resource allocation.

### 3. Anomaly Detection
Compare actuals to forecast to identify unusual business events or issues.

### 4. Trend Analysis
Analyze historical patterns (seasonality, growth) to understand business drivers.

### 5. Scenario Planning
Use forecast bounds for best-case, base-case, and worst-case scenarios.

## 🎓 Learning Outcomes

After working through this project, you'll understand:

✓ **Medallion Architecture**: Organizing data in Bronze/Silver/Gold layers  
✓ **Data Engineering**: Raw data generation, cleaning, feature engineering  
✓ **Time-Series Analysis**: Trend, seasonality, and pattern detection  
✓ **Snowflake ML**: Building forecasting models with SQL  
✓ **Business Analytics**: Creating views for reporting and dashboards  
✓ **Pipeline Orchestration**: Automating ETL with Python  
✓ **Data Quality**: Validation, monitoring, and metrics  

## 📝 Customization

### Modify Data Patterns

Edit `01_sales_raw_data_generator.sql` to change:
- **Growth rate**: Adjust `0.15` in base_sales formula
- **Seasonality**: Modify monthly effect values
- **Weekday pattern**: Change weekend/weekday multipliers
- **Noise level**: Adjust ±15 in UNIFORM() function

### Change Time Periods

```sql
-- Generate 3 years instead of 2
FROM TABLE(GENERATOR(ROWCOUNT => 1095))  -- 1095 days = 3 years
```

### Adjust Forecast Horizon

```sql
-- 180-day forecast instead of 90
FORECASTING_PERIODS => 180
```

## ⚙️ Configuration

### config/dev.yaml
```yaml
pipeline:
  bronze:
    enabled: true
  silver:
    enabled: true
  gold:
    enabled: true
```

### config/credentials.yaml
```yaml
snowflake:
  user: your_user
  password: your_password
  account: xy12345.us-east-1
  warehouse: compute_wh
  database: analytics_db
  schema: practice
  role: sysadmin
```

## 🔧 Troubleshooting

### "Connection failed"
- Verify Snowflake account and credentials
- Check warehouse is running
- Confirm role has necessary permissions

### "Insufficient privileges"
- Ensure role can CREATE DATABASE, SCHEMA, TABLE
- Check warehouse is available
- Verify account permissions

### "Memory exceeded"
- Increase warehouse size
- Reduce ROWCOUNT in GENERATOR() for fewer data points
- Run in smaller batches

### "Model training failed"
- Check silver_daily_sales has sufficient data (minimum 30 rows)
- Verify SALE_DATE and DAILY_REVENUE have no nulls
- Ensure date range is contiguous

## 📚 Additional Resources

- [Snowflake ML Documentation](https://docs.snowflake.com/en/guides/forecasting)
- [Time-Series Forecasting](https://docs.snowflake.com/en/user-guide/ml-powered-applications)
- [Medallion Architecture](https://databricks.com/blog/2022/06/24/use-the-medallion-lakehouse-architecture.html)

## 📋 Checklist for Production

- [ ] Validate forecast accuracy on holdout data
- [ ] Set up automated pipeline scheduling
- [ ] Implement monitoring and alerting
- [ ] Create dashboard for stakeholders
- [ ] Document business assumptions
- [ ] Test disaster recovery procedures
- [ ] Implement audit logging
- [ ] Set up data governance policies

## 🎯 Next Steps

1. **Explore the data**: Run the data exploration queries
2. **Check forecast quality**: Review SHOW_EVALUATION_METRICS()
3. **Create dashboards**: Connect BI tool to actuals_vs_forecast view
4. **Automate execution**: Schedule pipeline with Snowflake tasks
5. **Extend the model**: Add external features (marketing spend, holidays)

## 📞 Support

For questions or issues:
1. Review this documentation
2. Check the SQL templates for examples
3. Review logging output in `logs/sales_forecast.log`
4. Consult Snowflake documentation for specific errors

---

**Project Version**: 1.0.0  
**Last Updated**: April 15, 2026  
**Author**: Thiago F. A. Almeida
