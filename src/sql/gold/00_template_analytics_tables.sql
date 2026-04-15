-- GOLD LAYER: Business Analytics and Reporting
-- Purpose: Create aggregated, business-ready tables optimized for analytics
-- Pattern: Dimensional models (dimensions + facts), aggregate tables, views
--
-- Key Principles:
-- 1. Business logic and aggregations
-- 2. Fact and dimension tables (star schema pattern)
-- 3. Summary and aggregate tables for performance
-- 4. Materialized views for complex queries
-- 5. Optimized for reporting and BI tools

-- ============================================================================
-- TEMPLATE: Create Dimension Table
-- ============================================================================
-- Description: Date dimension for time-based analysis

CREATE OR REPLACE TABLE &{SCHEMA}.gold_dim_date (
    date_id INTEGER PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    week_of_year INTEGER,
    day_name VARCHAR(10),
    month_name VARCHAR(10),
    is_weekday BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE,
    _created_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Gold layer: Date dimension for time-based analysis';

-- ============================================================================
-- TEMPLATE: Create Fact Table
-- ============================================================================
-- Description: Transaction fact table for business metrics

CREATE OR REPLACE TABLE &{SCHEMA}.gold_fact_sales (
    -- Dimension keys (foreign keys)
    customer_id STRING NOT NULL,
    product_id STRING NOT NULL,
    date_id INTEGER NOT NULL,
    
    -- Measures
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2) DEFAULT 0,
    tax_amount DECIMAL(10,2) DEFAULT 0,
    net_amount DECIMAL(10,2) NOT NULL,
    
    -- Flags
    is_return BOOLEAN DEFAULT FALSE,
    is_cancelled BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    transaction_id STRING UNIQUE,
    _created_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    FOREIGN KEY (customer_id) REFERENCES &{SCHEMA}.gold_dim_customer(customer_id),
    FOREIGN KEY (product_id) REFERENCES &{SCHEMA}.gold_dim_product(product_id),
    FOREIGN KEY (date_id) REFERENCES &{SCHEMA}.gold_dim_date(date_id)
)
COMMENT = 'Gold layer: Sales fact table';

-- ============================================================================
-- TEMPLATE: Create Aggregate/Summary Table
-- ============================================================================
-- Description: Pre-aggregated metrics for dashboard performance

CREATE OR REPLACE TABLE &{SCHEMA}.gold_agg_daily_sales (
    date_id INTEGER NOT NULL,
    customer_id STRING,
    product_id STRING,
    
    -- Aggregated measures
    total_sales DECIMAL(15,2),
    total_quantity INTEGER,
    transaction_count INTEGER,
    average_transaction_value DECIMAL(10,2),
    
    -- Additional metrics
    new_customers INTEGER,
    repeat_customers INTEGER,
    
    _refresh_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY (date_id, customer_id, product_id)
)
COMMENT = 'Gold layer: Daily sales aggregates';

-- ============================================================================
-- TEMPLATE: Create Materialized View
-- ============================================================================
-- Description: Pre-built analytical view for complex queries

CREATE OR REPLACE MATERIALIZED VIEW &{SCHEMA}.gold_vw_customer_metrics AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.email,
    COUNT(DISTINCT f.transaction_id) as total_transactions,
    COUNT(DISTINCT f.date_id) as days_with_purchase,
    SUM(f.net_amount) as total_revenue,
    AVG(f.net_amount) as average_order_value,
    MAX(f.date_id) as last_purchase_date,
    MIN(f.date_id) as first_purchase_date,
    DATEDIFF(DAY, TO_DATE(MIN(f.date_id), 'YYYYMMDD'), TO_DATE(MAX(f.date_id), 'YYYYMMDD')) as customer_lifetime_days
FROM &{SCHEMA}.gold_dim_customer c
LEFT JOIN &{SCHEMA}.gold_fact_sales f ON c.customer_id = f.customer_id
WHERE c.is_active = TRUE
GROUP BY 
    c.customer_id,
    c.customer_name,
    c.email
COMMENT = 'Gold layer: Customer metrics view (materialized for performance)'
;

-- ============================================================================
-- TEMPLATE: Populate Aggregate Table
-- ============================================================================

INSERT OVERWRITE TABLE &{SCHEMA}.gold_agg_daily_sales
SELECT 
    f.date_id,
    f.customer_id,
    f.product_id,
    SUM(f.net_amount) as total_sales,
    SUM(f.quantity) as total_quantity,
    COUNT(*) as transaction_count,
    AVG(f.net_amount) as average_transaction_value,
    COUNT(DISTINCT CASE WHEN f.date_id = (SELECT MAX(date_id) FROM &{SCHEMA}.gold_dim_date) 
                        AND NOT EXISTS (SELECT 1 FROM &{SCHEMA}.gold_fact_sales f2 
                                       WHERE f2.customer_id = f.customer_id 
                                       AND f2.date_id < f.date_id)
                        THEN f.customer_id END) as new_customers,
    COUNT(DISTINCT CASE WHEN f.date_id = (SELECT MAX(date_id) FROM &{SCHEMA}.gold_dim_date)
                        AND EXISTS (SELECT 1 FROM &{SCHEMA}.gold_fact_sales f2 
                                   WHERE f2.customer_id = f.customer_id 
                                   AND f2.date_id < f.date_id)
                        THEN f.customer_id END) as repeat_customers,
    CURRENT_TIMESTAMP() as _refresh_date
FROM &{SCHEMA}.gold_fact_sales f
WHERE f.is_cancelled = FALSE
GROUP BY 
    f.date_id,
    f.customer_id,
    f.product_id
;

-- ============================================================================
-- TEMPLATE: Data Quality and Consistency Checks
-- ============================================================================

-- Verify referential integrity
SELECT 
    'Missing Customer' as issue,
    COUNT(*) as count
FROM &{SCHEMA}.gold_fact_sales f
LEFT JOIN &{SCHEMA}.gold_dim_customer c ON f.customer_id = c.customer_id
WHERE c.customer_id IS NULL

UNION ALL

SELECT 
    'Missing Product' as issue,
    COUNT(*) as count
FROM &{SCHEMA}.gold_fact_sales f
LEFT JOIN &{SCHEMA}.gold_dim_product p ON f.product_id = p.product_id
WHERE p.product_id IS NULL
;

-- Verify measure calculations
SELECT 
    COUNT(CASE WHEN net_amount != (total_amount - discount_amount + tax_amount) THEN 1 END) as amount_calc_errors,
    COUNT(CASE WHEN total_amount != (quantity * unit_price) THEN 1 END) as total_calc_errors
FROM &{SCHEMA}.gold_fact_sales
;
