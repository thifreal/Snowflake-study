-- ============================================================================
-- BRONZE LAYER: Raw Sales Data Generation
-- ============================================================================
-- Purpose: Generate mock daily sales data with realistic patterns
-- Pattern: Growth trend + Seasonality + Weekday effect + Random noise
-- Grain: One row per day
-- Retention: Full historical data
--
-- Pattern: 2 years of daily sales data (2023-01-01 to 2024-12-31)
-- - Growth trend: Revenue gradually increases over time
-- - Seasonality: Nov-Dec high (holidays), Jan-Feb low
-- - Weekday effect: Weekdays sell more than weekends
-- - Random noise: Small random variation for realistic patterns

CREATE OR REPLACE TABLE &{SCHEMA}.bronze_daily_sales (
    -- Business columns
    sale_date DATE NOT NULL,
    daily_revenue DECIMAL(10,2) NOT NULL,
    num_orders INTEGER NOT NULL,
    
    -- Metadata columns
    _load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_system STRING DEFAULT 'GENERATOR',
    _data_year INTEGER,
    
    PRIMARY KEY (sale_date)
)
COMMENT = 'Bronze layer: Raw daily sales data with growth, seasonality, and random variation';

-- ============================================================================
-- Generate Daily Sales Data with Realistic Patterns
-- ============================================================================

INSERT INTO &{SCHEMA}.bronze_daily_sales (sale_date, daily_revenue, num_orders, _load_date, _data_year)

-- Step 1: Generate one row per day for 730 days
WITH date_spine AS (
    SELECT DATEADD(DAY, SEQ4(), '2023-01-01'::DATE) AS sale_date
    FROM TABLE(GENERATOR(ROWCOUNT => 730))
),

-- Step 2: Calculate revenue components for each day
sales_components AS (
    SELECT
        sale_date,
        EXTRACT(YEAR FROM sale_date)::INTEGER AS year_val,
        
        -- Base sales with growth trend (increases by 0.15 per day)
        100 + (DATEDIFF(DAY, '2023-01-01'::DATE, sale_date) * 0.15) AS base_sales,
        
        -- Seasonal effect by month (Nov-Dec peak, Jan-Feb low)
        CASE EXTRACT(MONTH FROM sale_date)
            WHEN 1  THEN -15   -- January: post-holiday dip
            WHEN 2  THEN -10   -- February: still slow
            WHEN 3  THEN 0     -- March: neutral
            WHEN 4  THEN 5     -- April: slight uptick
            WHEN 5  THEN 10    -- May: spring boost
            WHEN 6  THEN 15    -- June: summer start
            WHEN 7  THEN 12    -- July: summer
            WHEN 8  THEN 10    -- August: summer winding down
            WHEN 9  THEN 8     -- September: back to school
            WHEN 10 THEN 15    -- October: pre-holiday
            WHEN 11 THEN 30    -- November: Black Friday / holiday
            WHEN 12 THEN 45    -- December: peak holiday season
        END AS seasonal_effect,
        
        -- Weekday effect (weekends lower)
        CASE DAYOFWEEK(sale_date)
            WHEN 0 THEN -20    -- Sunday
            WHEN 6 THEN -20    -- Saturday
            ELSE 10            -- Mon-Fri
        END AS weekday_effect,
        
        -- Random noise for realism
        UNIFORM(-15::FLOAT, 15::FLOAT, RANDOM()) AS noise
    FROM date_spine
)

-- Step 3: Combine all components into final revenue and order count
SELECT
    sale_date,
    ROUND(GREATEST(base_sales + seasonal_effect + weekday_effect + noise, 10), 2) AS daily_revenue,
    ROUND(
        GREATEST((base_sales + seasonal_effect + weekday_effect + noise) / 
                 UNIFORM(15, 35, RANDOM()), 1), 
        0
    )::INTEGER AS num_orders,
    CURRENT_TIMESTAMP() AS _load_date,
    'GENERATOR' AS _source_system,
    year_val AS _data_year
FROM sales_components
ORDER BY sale_date;

-- ============================================================================
-- Data Quality Checks
-- ============================================================================

-- Verify no gaps in data
SELECT 
    COUNT(DISTINCT sale_date) AS distinct_dates,
    COUNT(*) AS total_rows,
    DATEDIFF(DAY, MIN(sale_date), MAX(sale_date)) + 1 AS expected_days,
    CASE 
        WHEN COUNT(*) = DATEDIFF(DAY, MIN(sale_date), MAX(sale_date)) + 1 
        THEN 'PASS: No gaps in data'
        ELSE 'FAIL: Data gaps detected'
    END AS gap_check
FROM &{SCHEMA}.bronze_daily_sales;

-- Verify revenue is positive
SELECT 
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN daily_revenue > 0 THEN 1 END) AS positive_revenue,
    COUNT(CASE WHEN daily_revenue <= 0 THEN 1 END) AS non_positive_revenue,
    CASE 
        WHEN COUNT(CASE WHEN daily_revenue > 0 THEN 1 END) = COUNT(*) 
        THEN 'PASS: All revenue positive'
        ELSE 'FAIL: Negative or zero revenue found'
    END AS revenue_check
FROM &{SCHEMA}.bronze_daily_sales;

-- Log data load
SELECT 
    COUNT(*) AS record_count,
    MIN(sale_date) AS earliest_date,
    MAX(sale_date) AS latest_date,
    ROUND(SUM(daily_revenue), 2) AS total_revenue,
    ROUND(AVG(daily_revenue), 2) AS avg_daily_revenue,
    CURRENT_TIMESTAMP() AS load_timestamp
FROM &{SCHEMA}.bronze_daily_sales;
