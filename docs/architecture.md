# Architecture Documentation

## Medallion Architecture Pattern

This project implements the **Medallion Architecture** (also known as the Delta Lake architecture), which provides a structured approach to data engineering on Snowflake.

### Overview

The medallion architecture consists of three main layers:

```
Raw Data (Sources)
    ↓
[BRONZE] → Raw Ingestion
    ↓
[SILVER] → Cleansing & Standardization
    ↓
[GOLD] → Aggregation & Business Analytics
    ↓
Analytics, BI Tools, ML Models
```

## Layer Descriptions

### 1. Bronze Layer (Raw Data)

**Purpose**: Ingest and store raw data from source systems with minimal transformation.

**Characteristics**:
- One-to-one mapping with source system data
- Maintains all columns from the source
- Includes metadata columns (_load_date, _source_system, _file_name)
- Stores data in its original format
- Used for audit trails and compliance

**Key SQL Patterns**:
```sql
-- Create staging table for new data
CREATE TABLE bronze.stg_raw_<source>;

-- Load from staging to bronze
INSERT INTO bronze.<source>_data 
SELECT *, CURRENT_TIMESTAMP() as _load_date
FROM bronze.stg_raw_<source>;

-- Archive old records (optional)
CREATE TABLE bronze.<source>_archive AS
SELECT * FROM bronze.<source>_data
WHERE _load_date < DATEADD(DAY, -retention_days, CURRENT_DATE());
```

**Best Practices**:
- Use VARIANT data type for semi-structured data
- Keep history using clustering keys
- Implement basic null checks before loading
- Document source system mappings
- Monitor load volumes and error rates

### 2. Silver Layer (Cleaned & Standardized Data)

**Purpose**: Clean, deduplicate, and standardize data for downstream analytics.

**Characteristics**:
- Applies business rules and data quality checks
- Removes duplicates and invalid records
- Standardizes data types and formats
- Implements slowly changing dimensions (SCD)
- Single source of truth for business data

**Key SQL Patterns**:

#### Data Quality Checks
```sql
WITH quality_checks AS (
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN id IS NULL THEN 1 END) as null_checks,
        COUNT(DISTINCT id) as unique_records
    FROM silver.cleaned_data
)
SELECT *, 
    CASE WHEN null_checks = 0 THEN 'PASS' ELSE 'FAIL' END as quality_status
FROM quality_checks;
```

#### SCD Type 2 Implementation
```sql
-- Mark expired records
UPDATE silver.dim_customer
SET end_date = CURRENT_DATE() - 1, is_current = FALSE
WHERE is_current = TRUE AND customer_id IN (
    SELECT DISTINCT customer_id FROM bronze.customer_updates
);

-- Insert new versions
INSERT INTO silver.dim_customer
SELECT customer_id, ..., CURRENT_DATE() as effective_date, NULL as end_date, TRUE as is_current
FROM bronze.customer_updates;
```

**Best Practices**:
- Implement comprehensive data validation
- Document business logic transformations
- Use deterministic hashing for duplicate detection
- Maintain data lineage (source -> silver)
- Monitor data quality metrics

### 3. Gold Layer (Business Analytics)

**Purpose**: Create optimized, business-ready analytics tables and views.

**Characteristics**:
- Aggregate and summarized data
- Dimensional and fact tables (star schema)
- Pre-calculated metrics for dashboards
- Materialized views for complex queries
- Optimized for reporting and BI

**Key SQL Patterns**:

#### Dimension Tables
```sql
CREATE TABLE gold.dim_customer (
    customer_id STRING NOT NULL PRIMARY KEY,
    customer_name STRING NOT NULL,
    ...,
    effective_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE
);
```

#### Fact Tables
```sql
CREATE TABLE gold.fact_sales (
    customer_id STRING NOT NULL,
    product_id STRING NOT NULL,
    date_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES gold.dim_customer,
    FOREIGN KEY (product_id) REFERENCES gold.dim_product,
    FOREIGN KEY (date_id) REFERENCES gold.dim_date
);
```

#### Aggregate Tables
```sql
CREATE TABLE gold.agg_daily_sales AS
SELECT 
    date_id,
    product_id,
    SUM(amount) as total_sales,
    COUNT(*) as transaction_count,
    AVG(amount) as avg_sale
FROM gold.fact_sales
GROUP BY date_id, product_id;
```

**Best Practices**:
- Use surrogate keys for all dimensional tables
- Implement conformed dimensions for consistency
- Pre-calculate frequently used aggregates
- Add data quality checks at end of pipeline
- Document table purposes and refresh schedules

## Data Flow Example

### Customer Sales Pipeline

```
Bronze Layer:
├── bronze.stg_raw_customer_order (staging)
├── bronze.customer_order (raw data)
│
Silver Layer:
├── silver.dim_customer (cleaned customers)
├── silver.dim_product (cleaned products)
├── silver.fact_order (deduplicated orders)
│
Gold Layer:
├── gold.dim_customer (with SCD Type 2)
├── gold.dim_product
├── gold.dim_date
├── gold.fact_sales (conformed facts)
└── gold.agg_daily_sales (aggregated metrics)
```

## Performance Optimization

### Indexing Strategy
```sql
-- Create indexes on frequently filtered columns
CREATE INDEX idx_customer_email ON silver.dim_customer(email);
CREATE INDEX idx_order_customer ON gold.fact_sales(customer_id);

-- Cluster key for large tables
ALTER TABLE gold.fact_sales CLUSTER BY (date_id, customer_id);
```

### Materialized Views
```sql
-- Pre-compute complex aggregations
CREATE MATERIALIZED VIEW gold.vw_customer_ltv AS
SELECT 
    customer_id,
    SUM(amount) as lifetime_value,
    COUNT(DISTINCT order_id) as total_orders
FROM gold.fact_sales
GROUP BY customer_id;

-- Refresh schedule
ALTER MATERIALIZED VIEW gold.vw_customer_ltv SET 
CHANGE_TRACKING = ON;
```

### Query Optimization
- Use EXPLAIN PLAN to analyze queries
- Partition large tables by date
- Use approximate functions for large datasets
- Push predicates down in joins
- Consider query result caching

## Monitoring & Alerts

### Key Metrics
- Data freshness (last load time vs. current time)
- Record counts (row growth, anomalies)
- Data quality (null %, duplicate %, missing values)
- Pipeline execution time
- Error rates and failure causes

### Alerting
```sql
-- Alert if no data loaded today
SELECT CASE 
    WHEN MAX(_load_date)::DATE < CURRENT_DATE() 
    THEN 'ALERT: No data loaded today' 
    ELSE 'OK' END as status
FROM bronze.raw_data;
```

## Data Governance

### Naming Conventions
- **Tables**: `{layer}_{entity}_{type}` (e.g., `silver_customer_dim`)
- **Columns**: `snake_case` (e.g., `customer_id`, `created_date`)
- **Aggregates**: `agg_{dimension}_{metric}` (e.g., `agg_daily_sales`)
- **Staging**: `stg_{source}` (e.g., `stg_raw_orders`)

### Metadata Management
```sql
-- Document table purposes
COMMENT ON TABLE gold.fact_sales IS 'Sales transactions from all channels';
COMMENT ON COLUMN gold.fact_sales.amount IS 'Total sale amount after discounts';
```

## Disaster Recovery

### Backup Strategy
- Regular snapshots of gold layer tables
- Archive bronze layer monthly
- Maintain change logs for critical tables

### Version Control
```sql
-- Keep historical versions
CREATE TABLE silver.dim_customer_v1 CLONE silver.dim_customer 
BEFORE (STATEMENT => @clone_point);

-- Zero-copy cloning for testing
CREATE TABLE gold.fact_sales_test CLONE gold.fact_sales;
```

## References

- [Medallion Architecture](https://databricks.com/blog/2022/06/24/use-the-medallion-lakehouse-architecture.html)
- [Snowflake Best Practices](https://docs.snowflake.com/en/user-guide/best-practices.html)
- [Star Schema Design](https://en.wikipedia.org/wiki/Star_schema)
- [SCD Type 2 Pattern](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_Add_new_row)
