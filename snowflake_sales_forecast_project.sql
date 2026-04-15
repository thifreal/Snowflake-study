-- ============================================================================
-- SNOWFLAKE SALES FORECAST PROJECT
-- ============================================================================
-- Author:  Thiago F. A. Almeida
-- Date:    April 15, 2026
-- Purpose: End-to-end practice project that creates mock sales data,
--          explores it with analytical queries, trains a forecasting model
--          using Snowflake built-in ML, and generates future predictions.
--
-- Prerequisites:
--   - A Snowflake account with a role that can create databases
--   - An active warehouse (e.g., LATAM_LAB_WH)
--
-- How to use:
--   Run each section in order from top to bottom in a Snowflake worksheet.
--   Read the comments to understand what each step does.
-- ============================================================================


-- ============================================================================
-- SECTION 1: SETUP - Create Database and Schema
-- ============================================================================
-- A DATABASE is the top-level container in Snowflake (like a folder).
-- A SCHEMA is a sub-container inside a database (like a subfolder).
-- We create both to keep our project organized.

CREATE DATABASE IF NOT EXISTS SALES_FORECAST_LAB;
CREATE SCHEMA IF NOT EXISTS SALES_FORECAST_LAB.PRACTICE;

-- Set this as our working context so we dont have to type the full path
-- every time (optional, but convenient in worksheets).
USE DATABASE SALES_FORECAST_LAB;
USE SCHEMA PRACTICE;


-- ============================================================================
-- SECTION 2: CREATE MOCK SALES DATA
-- ============================================================================
-- We generate 2 years of fake daily sales data (Jan 2023 - Dec 2024).
-- The data has realistic patterns:
--   - A growth trend:   revenue gradually increases over time
--   - Seasonality:      Nov-Dec are high (holidays), Jan-Feb are low
--   - Weekday effect:   weekdays sell more than weekends
--   - Random noise:     small random variation to make it realistic
--
-- Key Snowflake concepts used:
--   GENERATOR()   -> creates a set of rows (here, 730 rows = 2 years)
--   DATEADD()     -> adds days to a start date to build a date series
--   UNIFORM()     -> generates random numbers within a range
--   GREATEST()    -> returns the largest value (used to avoid negatives)
--   CTE (WITH)    -> Common Table Expression, a temporary named result set

CREATE OR REPLACE TABLE DAILY_SALES AS

-- Step 1: Generate one row per day for 730 days
WITH date_spine AS (
    SELECT DATEADD(DAY, SEQ4(), '2023-01-01')::DATE AS sale_date
    FROM TABLE(GENERATOR(ROWCOUNT => 730))
),

-- Step 2: Calculate revenue components for each day
sales_data AS (
    SELECT
        sale_date,
        100 + (DATEDIFF(DAY, '2023-01-01', sale_date) * 0.15) AS base_sales,
        CASE MONTH(sale_date)
            WHEN 1  THEN -15   -- January:   post-holiday dip
            WHEN 2  THEN -10   -- February:  still slow
            WHEN 3  THEN 0     -- March:     neutral
            WHEN 4  THEN 5     -- April:     slight uptick
            WHEN 5  THEN 10    -- May:       spring boost
            WHEN 6  THEN 15    -- June:      summer start
            WHEN 7  THEN 12    -- July:      summer
            WHEN 8  THEN 10    -- August:    summer winding down
            WHEN 9  THEN 8     -- September: back to school
            WHEN 10 THEN 15    -- October:   pre-holiday
            WHEN 11 THEN 30    -- November:  Black Friday / holiday shopping
            WHEN 12 THEN 45    -- December:  peak holiday season
        END AS seasonal_effect,
        CASE
            WHEN DAYOFWEEK(sale_date) IN (0, 6) THEN -20  -- Sat/Sun
            ELSE 10                                         -- Mon-Fri
        END AS weekday_effect,
        UNIFORM(-15::FLOAT, 15::FLOAT, RANDOM()) AS noise
    FROM date_spine
)

-- Step 3: Combine all components into final revenue and order count
SELECT
    sale_date,
    ROUND(GREATEST(base_sales + seasonal_effect + weekday_effect + noise, 10), 2) AS daily_revenue,
    ROUND(GREATEST((base_sales + seasonal_effect + weekday_effect + noise) / UNIFORM(15, 35, RANDOM()), 1), 0)::INT AS num_orders
FROM sales_data
ORDER BY sale_date;


-- ============================================================================
-- SECTION 3: EXPLORE THE DATA
-- ============================================================================
-- Before building a model, always explore your data first!

-- 3a. Preview the first 10 rows
SELECT * FROM DAILY_SALES LIMIT 10;

-- 3b. Summary statistics
SELECT
    COUNT(*)                          AS total_days,
    MIN(sale_date)                    AS first_date,
    MAX(sale_date)                    AS last_date,
    ROUND(AVG(daily_revenue), 2)      AS avg_daily_revenue,
    ROUND(MIN(daily_revenue), 2)      AS min_daily_revenue,
    ROUND(MAX(daily_revenue), 2)      AS max_daily_revenue,
    SUM(num_orders)                   AS total_orders,
    ROUND(SUM(daily_revenue), 2)      AS total_revenue
FROM DAILY_SALES;

-- 3c. Monthly revenue breakdown (reveals seasonality and growth trend)
SELECT
    DATE_TRUNC('MONTH', sale_date)::DATE AS month,
    ROUND(SUM(daily_revenue), 2)         AS monthly_revenue,
    SUM(num_orders)                      AS monthly_orders,
    ROUND(AVG(daily_revenue), 2)         AS avg_daily_revenue
FROM DAILY_SALES
GROUP BY 1
ORDER BY 1;

-- 3d. Day-of-week analysis (confirms weekend vs weekday pattern)
SELECT
    DAYNAME(sale_date)                   AS day_name,
    DAYOFWEEK(sale_date)                 AS day_number,
    COUNT(*)                             AS num_days,
    ROUND(AVG(daily_revenue), 2)         AS avg_revenue
FROM DAILY_SALES
GROUP BY 1, 2
ORDER BY 2;


-- ============================================================================
-- SECTION 4: PREPARE DATA FOR THE FORECAST MODEL
-- ============================================================================
-- Snowflake FORECAST needs exactly:
--   1. A timestamp column (our dates)
--   2. A target column (the value to predict)
--
-- Any extra columns are treated as "exogenous features" (external signals).
-- Since we cant predict future NUM_ORDERS, we create a view without it.

CREATE OR REPLACE VIEW SALES_REVENUE_ONLY AS
SELECT SALE_DATE, DAILY_REVENUE
FROM DAILY_SALES;


-- ============================================================================
-- SECTION 5: TRAIN THE FORECASTING MODEL
-- ============================================================================
-- CREATE SNOWFLAKE.ML.FORECAST creates a machine learning model that:
--   - Automatically detects trends, seasonality, and patterns
--   - Uses algorithms optimized for time-series data
--   - Stores the trained model as a Snowflake object you can call later
--
-- SYSTEM$REFERENCE() is how you pass a table/view reference to ML functions.

CREATE OR REPLACE SNOWFLAKE.ML.FORECAST REVENUE_FORECAST_MODEL(
    INPUT_DATA        => SYSTEM$REFERENCE('VIEW', 'SALES_FORECAST_LAB.PRACTICE.SALES_REVENUE_ONLY'),
    TIMESTAMP_COLNAME => 'SALE_DATE',
    TARGET_COLNAME    => 'DAILY_REVENUE'
);


-- ============================================================================
-- SECTION 6: CHECK MODEL QUALITY
-- ============================================================================
-- Key metrics:
--   MAPE     -> Mean Absolute Percentage Error (lower = better, <10% is good)
--   MAE      -> Mean Absolute Error in dollars (lower = better)
--   MSE      -> Mean Squared Error (penalizes large errors more)
--   COVERAGE -> % of actuals that fall within the confidence interval

CALL REVENUE_FORECAST_MODEL!SHOW_EVALUATION_METRICS();


-- ============================================================================
-- SECTION 7: GENERATE THE FORECAST (90 days)
-- ============================================================================
-- The model returns:
--   TS          -> the future date
--   FORECAST    -> the predicted value
--   LOWER_BOUND -> lower end of 95% confidence interval
--   UPPER_BOUND -> upper end of 95% confidence interval

CREATE OR REPLACE TABLE REVENUE_FORECAST AS
SELECT *
FROM TABLE(
    REVENUE_FORECAST_MODEL!FORECAST(
        FORECASTING_PERIODS => 90
    )
);

-- Preview the forecast
SELECT
    TS::DATE               AS forecast_date,
    ROUND(FORECAST, 2)     AS forecasted_revenue,
    ROUND(LOWER_BOUND, 2)  AS lower_bound,
    ROUND(UPPER_BOUND, 2)  AS upper_bound
FROM REVENUE_FORECAST
ORDER BY TS
LIMIT 15;


-- ============================================================================
-- SECTION 8: COMBINE ACTUALS + FORECAST INTO ONE VIEW
-- ============================================================================
-- Common pattern: single view with historical data and predictions side by side.

CREATE OR REPLACE VIEW ACTUALS_VS_FORECAST AS
SELECT
    sale_date       AS date,
    daily_revenue   AS revenue,
    NULL::FLOAT     AS forecast_revenue,
    NULL::FLOAT     AS forecast_lower,
    NULL::FLOAT     AS forecast_upper,
    'ACTUAL'        AS data_type
FROM DAILY_SALES

UNION ALL

SELECT
    TS::DATE                 AS date,
    NULL::FLOAT              AS revenue,
    ROUND(FORECAST, 2)       AS forecast_revenue,
    ROUND(LOWER_BOUND, 2)    AS forecast_lower,
    ROUND(UPPER_BOUND, 2)    AS forecast_upper,
    'FORECAST'               AS data_type
FROM REVENUE_FORECAST
ORDER BY date;


-- ============================================================================
-- SECTION 9: USEFUL QUERIES
-- ============================================================================

-- 9a. Transition from actuals to forecast
SELECT * FROM ACTUALS_VS_FORECAST
WHERE date >= '2024-12-25'
ORDER BY date LIMIT 20;

-- 9b. Monthly forecast summary
SELECT
    DATE_TRUNC('MONTH', date)::DATE AS month,
    ROUND(AVG(forecast_revenue), 2) AS avg_daily_forecast,
    ROUND(SUM(forecast_revenue), 2) AS total_monthly_forecast
FROM ACTUALS_VS_FORECAST
WHERE data_type = 'FORECAST'
GROUP BY 1 ORDER BY 1;

-- 9c. Year-over-year: Jan 2024 actuals vs Jan 2025 forecast
SELECT 'Jan 2024 (Actual)' AS period, ROUND(SUM(revenue), 2) AS total_revenue
FROM ACTUALS_VS_FORECAST WHERE data_type = 'ACTUAL' AND date BETWEEN '2024-01-01' AND '2024-01-31'
UNION ALL
SELECT 'Jan 2025 (Forecast)', ROUND(SUM(forecast_revenue), 2)
FROM ACTUALS_VS_FORECAST WHERE data_type = 'FORECAST' AND date BETWEEN '2025-01-01' AND '2025-01-31';


-- ============================================================================
-- SECTION 10: CLEANUP (OPTIONAL - uncomment to delete everything)
-- ============================================================================
-- DROP VIEW  IF EXISTS ACTUALS_VS_FORECAST;
-- DROP VIEW  IF EXISTS SALES_REVENUE_ONLY;
-- DROP TABLE IF EXISTS REVENUE_FORECAST;
-- DROP TABLE IF EXISTS DAILY_SALES;
-- DROP SNOWFLAKE.ML.FORECAST IF EXISTS REVENUE_FORECAST_MODEL;
-- DROP SCHEMA IF EXISTS SALES_FORECAST_LAB.PRACTICE;
-- DROP DATABASE IF EXISTS SALES_FORECAST_LAB;


-- ============================================================================
-- OBJECTS CREATED
-- ============================================================================
-- | Object                     | Type     | Description                         |
-- |----------------------------|----------|-------------------------------------|
-- | DAILY_SALES                | Table    | 730 rows of mock daily sales data   |
-- | SALES_REVENUE_ONLY         | View     | Date + revenue only (model input)   |
-- | REVENUE_FORECAST_MODEL     | ML Model | Trained forecasting model           |
-- | REVENUE_FORECAST           | Table    | 90-day forecast with intervals      |
-- | ACTUALS_VS_FORECAST        | View     | Combined historical + forecast      |
--
-- ============================================================================
-- KEY SNOWFLAKE CONCEPTS COVERED (16 total)
-- ============================================================================
-- 1.  CREATE DATABASE / SCHEMA    -> Organizing objects
-- 2.  CREATE TABLE ... AS SELECT  -> Creating tables from queries (CTAS)
-- 3.  GENERATOR()                 -> Generating synthetic rows
-- 4.  DATEADD / DATEDIFF          -> Date arithmetic
-- 5.  CASE ... WHEN               -> Conditional logic
-- 6.  CTE (WITH ... AS)           -> Temporary named result sets
-- 7.  UNIFORM() / RANDOM()        -> Random number generation
-- 8.  GREATEST()                   -> Taking the max of multiple values
-- 9.  DATE_TRUNC()                -> Grouping dates by month/week/etc.
-- 10. ROUND() / ::INT / ::DATE    -> Type casting and rounding
-- 11. CREATE VIEW                 -> Virtual tables based on queries
-- 12. SYSTEM$REFERENCE()          -> Passing object references to functions
-- 13. SNOWFLAKE.ML.FORECAST       -> Built-in ML forecasting (no Python!)
-- 14. MODEL!METHOD() syntax       -> Calling methods on ML model objects
-- 15. UNION ALL                   -> Combining multiple result sets
-- 16. GROUP BY / ORDER BY         -> Aggregation and sorting
-- ============================================================================