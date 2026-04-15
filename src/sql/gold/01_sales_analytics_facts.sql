-- ============================================================================
-- GOLD LAYER: Analytics and Forecasting Tables
-- ============================================================================
-- Purpose: Business-ready analytics and ML forecasting tables
-- Grain: Daily sales facts optimized for reporting and ML
--
-- Objects created:
-- 1. gold_fact_daily_sales: Analytics-ready facts
-- 2. gold_vw_exploration: Exploratory analysis view
-- 3. gold_vw_actuals_forecast: Combined actual vs forecast view

-- ============================================================================
-- Gold Fact: Daily Sales (Analytics Ready)
-- ============================================================================

CREATE OR REPLACE TABLE &{SCHEMA}.gold_fact_daily_sales AS
SELECT
    -- Date key
    EXTRACT(YEAR FROM sale_date)::INTEGER * 10000 + 
    EXTRACT(MONTH FROM sale_date)::INTEGER * 100 + 
    EXTRACT(DAY FROM sale_date)::INTEGER AS date_key,
    
    -- Business columns
    sale_date,
    daily_revenue,
    num_orders,
    ROUND(daily_revenue / NULLIF(num_orders, 0), 2) AS revenue_per_order,
    
    -- Dimension references
    EXTRACT(YEAR FROM sale_date)::INTEGER AS year,
    EXTRACT(MONTH FROM sale_date)::INTEGER AS month,
    EXTRACT(QUARTER FROM sale_date)::INTEGER AS quarter,
    DAYOFWEEK(sale_date)::INTEGER AS day_of_week,
    WEEKOFYEAR(sale_date)::INTEGER AS week_of_year,
    
    -- Calculated metrics
    ROUND(SUM(daily_revenue) OVER (
        ORDER BY sale_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS revenue_7day_rolling,
    
    ROUND(SUM(daily_revenue) OVER (
        ORDER BY sale_date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ), 2) AS revenue_30day_rolling,
    
    ROUND(SUM(daily_revenue) OVER (
        PARTITION BY EXTRACT(YEAR FROM sale_date)::INTEGER, 
                     EXTRACT(MONTH FROM sale_date)::INTEGER
        ORDER BY sale_date
    ), 2) AS revenue_month_to_date,
    
    -- YoY comparison
    LAG(daily_revenue) OVER (
        ORDER BY sale_date 
        ROWS BETWEEN 365 PRECEDING AND CURRENT ROW
    ) AS revenue_yoy_prior,
    
    ROUND(
        (daily_revenue - LAG(daily_revenue) OVER (
            ORDER BY sale_date 
            ROWS BETWEEN 365 PRECEDING AND CURRENT ROW
        )) / NULLIF(LAG(daily_revenue) OVER (
            ORDER BY sale_date 
            ROWS BETWEEN 365 PRECEDING AND CURRENT ROW
        ), 0) * 100, 2
    ) AS revenue_yoy_pct_change,
    
    -- Flags
    CASE WHEN DAYOFWEEK(sale_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN EXTRACT(MONTH FROM sale_date) IN (11, 12) THEN TRUE ELSE FALSE END AS is_holiday_season,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _created_date,
    'silver_daily_sales' AS _source_table
    
FROM &{SCHEMA}.silver_daily_sales
ORDER BY sale_date;

COMMENT ON TABLE &{SCHEMA}.gold_fact_daily_sales IS 'Gold layer: Daily sales facts with rolling aggregates and YoY metrics';

-- ============================================================================
-- Exploratory Analysis View
-- ============================================================================

CREATE OR REPLACE VIEW &{SCHEMA}.gold_vw_sales_exploration AS
SELECT
    'Total Records' AS metric,
    NULL::DATE AS date_dimension,
    NULL::FLOAT AS value_numeric,
    COUNT(*)::VARCHAR AS value_text
FROM &{SCHEMA}.gold_fact_daily_sales
UNION ALL
SELECT
    'Date Range',
    NULL::DATE,
    NULL::FLOAT,
    MIN(sale_date)::VARCHAR || ' to ' || MAX(sale_date)::VARCHAR
FROM &{SCHEMA}.gold_fact_daily_sales
UNION ALL
SELECT
    'Average Daily Revenue',
    NULL::DATE,
    ROUND(AVG(daily_revenue), 2),
    NULL::VARCHAR
FROM &{SCHEMA}.gold_fact_daily_sales
UNION ALL
SELECT
    'Total Revenue',
    NULL::DATE,
    ROUND(SUM(daily_revenue), 2),
    NULL::VARCHAR
FROM &{SCHEMA}.gold_fact_daily_sales
UNION ALL
SELECT
    'Min Daily Revenue',
    NULL::DATE,
    ROUND(MIN(daily_revenue), 2),
    NULL::VARCHAR
FROM &{SCHEMA}.gold_fact_daily_sales
UNION ALL
SELECT
    'Max Daily Revenue',
    NULL::DATE,
    ROUND(MAX(daily_revenue), 2),
    NULL::VARCHAR
FROM &{SCHEMA}.gold_fact_daily_sales
ORDER BY metric;

COMMENT ON VIEW &{SCHEMA}.gold_vw_sales_exploration IS 'Exploratory data analysis summary metrics';

-- ============================================================================
-- Monthly Summary for Reporting (Pre-aggregated)
-- ============================================================================

CREATE OR REPLACE TABLE &{SCHEMA}.gold_agg_monthly_sales AS
SELECT
    EXTRACT(YEAR FROM sale_date)::INTEGER * 100 + 
    EXTRACT(MONTH FROM sale_date)::INTEGER AS month_key,
    
    DATE_TRUNC('MONTH', sale_date)::DATE AS month_date,
    EXTRACT(YEAR FROM sale_date)::INTEGER AS year,
    EXTRACT(MONTH FROM sale_date)::INTEGER AS month,
    
    COUNT(*) AS num_days,
    ROUND(SUM(daily_revenue), 2) AS total_revenue,
    ROUND(AVG(daily_revenue), 2) AS avg_daily_revenue,
    ROUND(STDDEV(daily_revenue), 2) AS stddev_daily_revenue,
    ROUND(MIN(daily_revenue), 2) AS min_daily_revenue,
    ROUND(MAX(daily_revenue), 2) AS max_daily_revenue,
    
    SUM(num_orders) AS total_orders,
    ROUND(AVG(num_orders), 2) AS avg_daily_orders,
    ROUND(SUM(daily_revenue) / SUM(num_orders), 2) AS revenue_per_order,
    
    COUNT(CASE WHEN is_weekend THEN 1 END) AS weekend_days,
    COUNT(CASE WHEN NOT is_weekend THEN 1 END) AS weekday_days,
    
    ROUND(SUM(CASE WHEN is_weekend THEN daily_revenue ELSE 0 END), 2) AS weekend_revenue,
    ROUND(SUM(CASE WHEN NOT is_weekend THEN daily_revenue ELSE 0 END), 2) AS weekday_revenue,
    
    CURRENT_TIMESTAMP() AS _refresh_date
    
FROM &{SCHEMA}.gold_fact_daily_sales
GROUP BY 1, 2, 3, 4
ORDER BY month_date;

COMMENT ON TABLE &{SCHEMA}.gold_agg_monthly_sales IS 'Pre-aggregated monthly sales metrics for dashboard performance';

-- ============================================================================
-- Quarterly Summary for Executive Reporting
-- ============================================================================

CREATE OR REPLACE TABLE &{SCHEMA}.gold_agg_quarterly_sales AS
SELECT
    EXTRACT(YEAR FROM sale_date)::INTEGER * 10 + 
    EXTRACT(QUARTER FROM sale_date)::INTEGER AS quarter_key,
    
    EXTRACT(YEAR FROM sale_date)::INTEGER AS year,
    EXTRACT(QUARTER FROM sale_date)::INTEGER AS quarter,
    
    MIN(sale_date) AS quarter_start_date,
    MAX(sale_date) AS quarter_end_date,
    COUNT(*) AS num_days,
    
    ROUND(SUM(daily_revenue), 2) AS total_revenue,
    ROUND(AVG(daily_revenue), 2) AS avg_daily_revenue,
    ROUND(SUM(daily_revenue) / COUNT(*), 2) AS avg_revenue,
    
    SUM(num_orders) AS total_orders,
    ROUND(AVG(num_orders), 2) AS avg_daily_orders,
    ROUND(SUM(daily_revenue) / SUM(num_orders), 2) AS revenue_per_order,
    
    -- Growth metrics
    ROUND(MIN(daily_revenue), 2) AS min_daily_revenue,
    ROUND(MAX(daily_revenue), 2) AS max_daily_revenue,
    ROUND(MAX(daily_revenue) - MIN(daily_revenue), 2) AS revenue_swing,
    
    CURRENT_TIMESTAMP() AS _refresh_date
    
FROM &{SCHEMA}.gold_fact_daily_sales
GROUP BY 1, 2, 3
ORDER BY year DESC, quarter DESC;

COMMENT ON TABLE &{SCHEMA}.gold_agg_quarterly_sales IS 'Pre-aggregated quarterly sales metrics for executive dashboards';

-- ============================================================================
-- Verifications and Data Quality
-- ============================================================================

-- Check referential integrity
SELECT 
    'gold_fact_daily_sales consistency' AS check_name,
    COUNT(DISTINCT date_key) AS distinct_date_keys,
    COUNT(DISTINCT sale_date) AS distinct_dates,
    CASE 
        WHEN COUNT(DISTINCT date_key) = COUNT(DISTINCT sale_date) 
        THEN 'PASS' 
        ELSE 'FAIL' 
    END AS status
FROM &{SCHEMA}.gold_fact_daily_sales;

-- Verify aggregates consistency
SELECT 
    'Monthly aggregates completeness' AS check_name,
    COUNT(DISTINCT month_date) AS aggregated_months,
    (SELECT COUNT(DISTINCT DATE_TRUNC('MONTH', sale_date)::DATE) 
     FROM &{SCHEMA}.gold_fact_daily_sales) AS expected_months,
    CASE 
        WHEN COUNT(DISTINCT month_date) = 
             (SELECT COUNT(DISTINCT DATE_TRUNC('MONTH', sale_date)::DATE) 
              FROM &{SCHEMA}.gold_fact_daily_sales)
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM &{SCHEMA}.gold_agg_monthly_sales;
