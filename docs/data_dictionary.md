# Data Dictionary

## Overview

This document describes all tables, views, and key attributes in the Snowflake analytics data warehouse following the medallion architecture pattern.

## Bronze Layer (Raw Data)

### bronze.raw_data
**Description**: Raw customer order data from source system  
**Refresh Cycle**: Daily (incremental)  
**Source System**: ERP System  
**Owner**: Data Engineering  
**Contact**: data-eng@company.com  

| Column | Type | Description | Nullable | Notes |
|--------|------|-------------|----------|-------|
| id | STRING | Unique order identifier | No | PK from source |
| customer_id | STRING | Customer identifier | Yes | May be missing in raw data |
| order_date | TIMESTAMP_NTZ | Order date from source | Yes | Not nullable in business |
| amount | NUMBER | Order amount in source currency | Yes | Requires conversion |
| status | STRING | Order status | Yes | Values: PENDING, COMPLETED, CANCELLED |
| _load_date | TIMESTAMP_NTZ | Date record was loaded | No | Pipeline metadata |
| _source_system | STRING | Source system identifier | No | Pipeline metadata |
| _file_name | STRING | Source file name | Yes | For audit trail |

**Data Quality Rules**:
- id is always populated
- amount should be numeric and positive
- Load date should be recent (within 24 hours for daily loads)

---

## Silver Layer (Cleaned Data)

### silver.dim_customer
**Description**: Cleaned and deduplicated customer dimension with SCD Type 2 history tracking  
**Refresh Cycle**: Daily  
**Grain**: One row per customer version  
**Owner**: Analytics  
**Contact**: analytics@company.com  

| Column | Type | Description | Nullable | Notes |
|--------|------|-------------|----------|-------|
| customer_id | STRING | Unique customer identifier | No | PK |
| customer_name | STRING | Customer full name | No | Standardized |
| email | STRING | Customer email address | Yes | Deduplicated |
| phone | STRING | Customer phone number | Yes | Standardized format |
| address | STRING | Street address | Yes | |
| city | STRING | City name | Yes | |
| state | STRING | State or province | Yes | |
| postal_code | STRING | Postal code | Yes | |
| country | STRING | Country | Yes | |
| is_valid_email | BOOLEAN | Email validation flag | Yes | Regex validated |
| is_active | BOOLEAN | Customer active flag | No | Business flag |
| effective_date | DATE | When this version became active | No | SCD Type 2 |
| end_date | DATE | When this version was superseded | Yes | NULL if current |
| is_current | BOOLEAN | Flag for current version | No | For fast filtering |
| _created_date | TIMESTAMP_NTZ | When record entered silver layer | No | |
| _updated_date | TIMESTAMP_NTZ | When record was updated | No | |
| _source_table | STRING | Source bronze table | No | Lineage |

**Data Quality Rules**:
- customer_id is unique per effective_date
- No overlapping date ranges for same customer
- is_current = TRUE only for end_date IS NULL
- email must be validated with regex pattern

**Example Query**:
```sql
-- Get current customers
SELECT * FROM silver.dim_customer 
WHERE is_current = TRUE AND is_active = TRUE;

-- Get customer history
SELECT * FROM silver.dim_customer 
WHERE customer_id = 'CUST123' 
ORDER BY effective_date DESC;
```

---

### silver.fact_order
**Description**: Deduplicated order detail facts  
**Refresh Cycle**: Daily (incremental)  
**Grain**: One row per order line item  
**Owner**: Analytics  

| Column | Type | Description | Nullable | Notes |
|--------|------|-------------|----------|-------|
| order_id | STRING | Unique order identifier | No | PK from bronze |
| order_line_id | STRING | Line item identifier | No | Composite with order_id |
| customer_id | STRING | Customer identifier | No | FK to dim_customer |
| product_id | STRING | Product identifier | No | FK to dim_product |
| order_date | DATE | Order date | No | Standardized date |
| quantity | NUMBER | Quantity ordered | No | Must be positive |
| unit_price | NUMBER(10,2) | Price per unit | No | In standard currency |
| line_amount | NUMBER(10,2) | Quantity × unit price | No | Calculated |
| discount_percent | NUMBER(5,2) | Discount percentage | Yes | 0-100 |
| final_amount | NUMBER(10,2) | Amount after discount | No | Calculated |
| is_return | BOOLEAN | Return flag | No | Default FALSE |
| is_cancelled | BOOLEAN | Cancellation flag | No | Default FALSE |
| _load_date | TIMESTAMP_NTZ | Load date | No | |
| _dedup_flag | STRING | Deduplication indicator | Yes | For audit |

**Data Quality Rules**:
- No duplicate order_line_ids
- quantity >= 0, unit_price >= 0
- final_amount = line_amount × (1 - discount_percent/100)
- Cannot be both return and cancelled

---

## Gold Layer (Analytics)

### gold.dim_customer
**Description**: Business-ready customer dimension optimized for reporting  
**Refresh Cycle**: Daily  
**Owner**: BI Team  

| Column | Type | Description | Nullable | Notes |
|--------|------|-------------|----------|-------|
| customer_key | NUMBER | Surrogate key | No | PK - Snowflake identity |
| customer_id | STRING | Natural key | No | From source system |
| customer_name | STRING | Full name | No | |
| email | STRING | Email address | Yes | Unique |
| primary_phone | STRING | Main phone number | Yes | |
| customer_segment | STRING | Business segment | Yes | Values: PREMIUM, STANDARD, BASIC |
| lifetime_value | NUMBER(10,2) | Total customer LTV | Yes | Pre-calculated |
| total_orders | NUMBER | Total orders | No | Pre-calculated |
| is_active | BOOLEAN | Currently active | No | |
| effective_date | DATE | Effective date | No | SCD Type 2 |
| end_date | DATE | End date | Yes | NULL if current |
| is_current | BOOLEAN | Current version flag | No | |
| _created_date | TIMESTAMP_NTZ | Record creation | No | |

**Purpose**: Used in customer segment analysis, LTV calculations, and reporting dashboards  
**Sample Queries**:
```sql
-- Customer segment analysis
SELECT 
    customer_segment,
    COUNT(*) as customer_count,
    AVG(lifetime_value) as avg_ltv
FROM gold.dim_customer WHERE is_current = TRUE
GROUP BY customer_segment;
```

---

### gold.dim_product
**Description**: Product dimension table  
**Grain**: One row per unique product  
**Owner**: Product Team  

| Column | Type | Description |
|--------|------|-------------|
| product_key | NUMBER | Surrogate key |
| product_id | STRING | Natural key |
| product_name | STRING | Product name |
| category | STRING | Product category |
| sub_category | STRING | Sub category |
| unit_price | NUMBER(10,2) | Current unit price |
| is_active | BOOLEAN | Active in current period |

---

### gold.dim_date
**Description**: Date dimension for time-based analysis  
**Grain**: One row per calendar day  
**Coverage**: 2020-01-01 to 2030-12-31  

| Column | Type | Description |
|--------|------|-------------|
| date_id | NUMBER | Date as integer (YYYYMMDD) |
| calendar_date | DATE | Actual date |
| year | NUMBER | Calendar year |
| quarter | NUMBER | Quarter (1-4) |
| month | NUMBER | Month (1-12) |
| day_of_month | NUMBER | Day in month (1-31) |
| day_of_week | NUMBER | Day of week (1-7, Mon-Sun) |
| week_of_year | NUMBER | Week number (1-53) |
| day_name | VARCHAR(10) | Day of week name |
| month_name | VARCHAR(10) | Month name |
| is_weekday | BOOLEAN | TRUE if Mon-Fri |
| is_holiday | BOOLEAN | TRUE if holiday |

---

### gold.fact_sales
**Description**: Sales transactions fact table  
**Grain**: One row per order line item  
**Refresh Cycle**: Daily (incremental)  

| Column | Type | Description | Nullable |
|--------|------|-------------|----------|
| customer_key | NUMBER | FK to dim_customer | No |
| product_key | NUMBER | FK to dim_product | No |
| date_key | NUMBER | FK to dim_date | No |
| quantity | NUMBER | Units sold | No |
| unit_price | NUMBER(10,2) | Price per unit | No |
| line_amount | NUMBER(10,2) | Before discount/tax | No |
| discount_amount | NUMBER(10,2) | Discount applied | No |
| tax_amount | NUMBER(10,2) | Tax charged | No |
| net_amount | NUMBER(10,2) | Final order amount | No |
| is_return | BOOLEAN | Return indicator | No |
| is_cancelled | BOOLEAN | Cancellation indicator | No |

---

### gold.agg_daily_sales
**Description**: Pre-aggregated daily sales metrics for dashboard performance  
**Grain**: One row per day per customer per product  
**Refresh Cycle**: Daily (overnight job)  
**Update Strategy**: Full refresh (TRUNCATE + INSERT)  

| Column | Type | Description |
|--------|------|-------------|
| date_key | NUMBER | Date identifier |
| customer_key | NUMBER | Customer identifier |
| product_key | NUMBER | Product identifier |
| total_sales | NUMBER(15,2) | Total revenue for day |
| total_quantity | NUMBER | Total units sold |
| transaction_count | NUMBER | Number of transactions |
| average_transaction_value | NUMBER(10,2) | Avg sale amount |
| new_customers | NUMBER | New customers acquired |
| repeat_customers | NUMBER | Returning customers |

**Performance Notes**:
- Table is clustered by date_key and customer_key
- Indexes on (date_key, customer_key)
- Materialized view refreshes nightly

---

### gold.vw_customer_metrics (Materialized View)
**Description**: Pre-calculated customer-level metrics  
**Refresh Cycle**: Nightly (11 PM)  

| Column | Type | Description |
|--------|------|-------------|
| customer_key | NUMBER | Customer reference |
| customer_name | STRING | Customer display name |
| total_transactions | NUMBER | Lifetime transaction count |
| total_revenue | NUMBER(15,2) | Lifetime revenue |
| average_order_value | NUMBER(10,2) | Average order amount |
| last_purchase_date | DATE | Most recent purchase |
| customer_lifetime_days | NUMBER | Days as customer |

---

## Conformed Dimensions

### Conformed Dimension: Date
Used across all fact tables for consistent time-based filtering and grouping.

### Conformed Dimension: Customer
Used across multiple fact tables to ensure consistent customer metrics.

---

## Metadata Tables

### audit.pipeline_log
**Purpose**: Track data pipeline executions  
**Retention**: 90 days  

| Column | Type | Description |
|--------|------|-------------|
| load_id | NUMBER | Unique load identifier |
| layer | VARCHAR | bronze/silver/gold |
| table_name | VARCHAR | Affected table |
| row_count | NUMBER | Rows loaded |
| execution_time | NUMBER | Seconds to execute |
| status | VARCHAR | SUCCESS/FAILED |
| error_message | VARCHAR | Error details if failed |
| load_timestamp | TIMESTAMP | When load executed |

---

## Refresh Schedule

| Layer | Table | Frequency | Time (UTC) |
|-------|-------|-----------|-----------|
| Bronze | raw_data | Daily | 02:00 |
| Silver | dim_customer | Daily | 03:00 |
| Silver | fact_order | Daily | 03:30 |
| Gold | dim_* | Daily | 04:00 |
| Gold | fact_sales | Daily | 04:30 |
| Gold | agg_daily_sales | Daily | 05:00 |

---

## Data Lineage

```
Source Systems
    ↓
[BRONZE] raw_data
    ↓
[SILVER] dim_customer, fact_order  
    ↓
[GOLD] dim_customer, fact_sales, agg_daily_sales, vw_customer_metrics
    ↓
BI Tools, Dashboards, ML Models
```

---

## Related Documents

- Architecture: [architecture.md](architecture.md)
- Setup Guide: [setup.md](setup.md)
- SQL Templates: [../src/sql/](../src/sql/)

---

**Last Updated**: April 15, 2026  
**Version**: 1.0.0
