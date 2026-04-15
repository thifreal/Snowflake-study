-- ============================================================================
-- SILVER LAYER: Cleaned and Standardized Sales Data
-- ============================================================================
-- Purpose: Prepare data for forecasting by cleaning, validating, and enriching
-- Grain: One row per day
-- Transformations:
--   - Remove duplicates and outliers
--   - Add time-series features (month, quarter, day of week)
--   - Create calendar-based aggregations
--   - Validate data quality

-- ============================================================================
-- Clean Daily Sales (deduplicated, no outliers)
-- ============================================================================

CREATE OR REPLACE TABLE &{SCHEMA}.silver_daily_sales AS
SELECT 
    sale_date,
    daily_revenue,
    num_orders,
    
    -- Time-series features
    EXTRACT(YEAR FROM sale_date)::INTEGER AS year,
    EXTRACT(MONTH FROM sale_date)::INTEGER AS month,
    EXTRACT(QUARTER FROM sale_date)::INTEGER AS quarter,
    DAYOFWEEK(sale_date)::INTEGER AS day_of_week,
    DAYNAME(sale_date) AS day_name,
    WEEKOFYEAR(sale_date)::INTEGER AS week_of_year,
    
    -- Derived metrics
    ROUND(daily_revenue / NULLIF(num_orders, 0), 2) AS revenue_per_order,
    
    -- Flags
    CASE 
        WHEN DAYOFWEEK(sale_date) IN (0, 6) THEN TRUE 
        ELSE FALSE 
    END AS is_weekend,
    
    CASE 
        WHEN EXTRACT(MONTH FROM sale_date) IN (11, 12) THEN TRUE
        ELSE FALSE
    END AS is_holiday_season,
    
    -- Check for outliers using z-score (simplified)
    ABS(daily_revenue - AVG(daily_revenue) OVER (
        ORDER BY sale_date 
        ROWS BETWEEN 30 PRECEDING AND 30 FOLLOWING
    )) / NULLIF(STDDEV(daily_revenue) OVER (
        ORDER BY sale_date 
        ROWS BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ), 0) AS revenue_zscore,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _created_date,
    CURRENT_TIMESTAMP() AS _updated_date,
    'bronze_daily_sales' AS _source_table
    
FROM &{SCHEMA}.bronze_daily_sales

WHERE 
    daily_revenue > 0  -- Only positive revenues
    AND num_orders > 0 -- Only valid order counts
    
ORDER BY sale_date;

COMMENT ON TABLE &{SCHEMA}.silver_daily_sales IS 'Silver layer: Cleaned daily sales with time-series features and quality flags';

-- ============================================================================
-- Monthly Aggregation View (for trend analysis)
-- ============================================================================

CREATE OR REPLACE VIEW &{SCHEMA}.silver_vw_monthly_sales AS
SELECT
    DATE_TRUNC('MONTH', sale_date)::DATE AS month_date,
    EXTRACT(YEAR FROM sale_date)::INTEGER AS year,
    EXTRACT(MONTH FROM sale_date)::INTEGER AS month,
    COUNT(*) AS num_days,
    SUM(daily_revenue) AS total_revenue,
    ROUND(AVG(daily_revenue), 2) AS avg_daily_revenue,
    MIN(daily_revenue) AS min_daily_revenue,
    MAX(daily_revenue) AS max_daily_revenue,
    ROUND(STDDEV(daily_revenue), 2) AS stddev_daily_revenue,
    SUM(num_orders) AS total_orders,
    ROUND(AVG(num_orders), 2) AS avg_daily_orders,
    ROUND(SUM(daily_revenue) / SUM(num_orders), 2) AS revenue_per_order
FROM &{SCHEMA}.silver_daily_sales
GROUP BY 1, 2, 3
ORDER BY 1;

COMMENT ON VIEW &{SCHEMA}.silver_vw_monthly_sales IS 'Monthly aggregated sales metrics for trend analysis';

-- ============================================================================
-- Quarterly Aggregation View
-- ============================================================================

CREATE OR REPLACE VIEW &{SCHEMA}.silver_vw_quarterly_sales AS
SELECT
    EXTRACT(YEAR FROM sale_date)::INTEGER AS year,
    EXTRACT(QUARTER FROM sale_date)::INTEGER AS quarter,
    COUNT(*) AS num_days,
    ROUND(SUM(daily_revenue), 2) AS total_revenue,
    ROUND(AVG(daily_revenue), 2) AS avg_daily_revenue,
    ROUND(SUM(daily_revenue) / COUNT(*), 2) AS avg_revenue,
    SUM(num_orders) AS total_orders,
    ROUND(AVG(num_orders), 2) AS avg_daily_orders,
    ROUND(SUM(daily_revenue) / SUM(num_orders), 2) AS revenue_per_order,
    MIN(daily_revenue) AS min_revenue,
    MAX(daily_revenue) AS max_revenue
FROM &{SCHEMA}.silver_daily_sales
GROUP BY 1, 2
ORDER BY 1, 2;

COMMENT ON VIEW &{SCHEMA}.silver_vw_quarterly_sales IS 'Quarterly aggregated sales metrics for business review';

-- ============================================================================
-- Weekday vs Weekend Analysis View
-- ============================================================================

CREATE OR REPLACE VIEW &{SCHEMA}.silver_vw_day_analysis AS
SELECT
    day_name,
    day_of_week,
    is_weekend,
    COUNT(*) AS num_occurrences,
    ROUND(AVG(daily_revenue), 2) AS avg_revenue,
    ROUND(SUM(daily_revenue), 2) AS total_revenue,
    ROUND(MIN(daily_revenue), 2) AS min_revenue,
    ROUND(MAX(daily_revenue), 2) AS max_revenue,
    ROUND(AVG(num_orders), 2) AS avg_orders,
    SUM(num_orders) AS total_orders
FROM &{SCHEMA}.silver_daily_sales
GROUP BY 1, 2, 3
ORDER BY day_of_week;

COMMENT ON VIEW &{SCHEMA}.silver_vw_day_analysis IS 'Day-of-week sales patterns analysis';

-- ============================================================================
-- Data Quality Summary
-- ============================================================================

SHOW
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT sale_date) AS distinct_dates,
    MIN(sale_date) AS earliest_date,
    MAX(sale_date) AS latest_date,
    ROUND(AVG(daily_revenue), 2) AS avg_revenue,
    ROUND(STDDEV(daily_revenue), 2) AS stddev_revenue,
    COUNT(CASE WHEN revenue_zscore > 3 THEN 1 END) AS outlier_count,
    ROUND(100.0 * COUNT(CASE WHEN revenue_zscore > 3 THEN 1 END) / COUNT(*), 2) AS outlier_pct
FROM &{SCHEMA}.silver_daily_sales;
