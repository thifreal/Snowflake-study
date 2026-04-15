-- ============================================================================
-- GOLD LAYER: Forecasting with Snowflake ML
-- ============================================================================
-- Purpose: Train forecasting model and generate predictions
-- Prerequisites: Historical data in silver_daily_sales
--
-- Key Snowflake ML Concepts:
--   SNOWFLAKE.ML.FORECAST - Creates time-series forecasting models
--   SYSTEM$REFERENCE() - References tables/views for ML functions
--   !FORECAST() - Generates predictions
--   !SHOW_EVALUATION_METRICS() - Displays model evaluation

-- ============================================================================
-- Step 1: Prepare view for model input (forecast requires only date + target)
-- ============================================================================

CREATE OR REPLACE VIEW &{SCHEMA}.gold_vw_forecast_input AS
SELECT 
    sale_date,
    daily_revenue
FROM &{SCHEMA}.silver_daily_sales
ORDER BY sale_date;

COMMENT ON VIEW &{SCHEMA}.gold_vw_forecast_input IS 'Input view for Snowflake ML forecasting model';

-- ============================================================================
-- Step 2: Train Forecasting Model
-- ============================================================================
-- This model automatically:
--   - Detects trends and seasonality patterns
--   - Handles missing data
--   - Applies state-of-the-art time-series algorithms
--   - Estimates confidence intervals
--
-- Replace this model if retraining is needed

CREATE OR REPLACE SNOWFLAKE.ML.FORECAST gold_sales_forecast_model(
    INPUT_DATA        => SYSTEM$REFERENCE('VIEW', '&{SCHEMA}.gold_vw_forecast_input'),
    TIMESTAMP_COLNAME => 'SALE_DATE',
    TARGET_COLNAME    => 'DAILY_REVENUE'
)
COMMENT = 'Snowflake ML Time-series Forecast Model for daily revenue predictions';

-- ============================================================================
-- Step 3: Check Model Quality Metrics
-- ============================================================================
-- Key metrics explained:
--   MAPE     -> Mean Absolute Percentage Error (lower = better, <10% is good)
--   MAE      -> Mean Absolute Error in dollars (lower = better)
--   MSE      -> Mean Squared Error (penalizes large errors)
--   RMSE     -> Root Mean Squared Error
--   COVERAGE -> % of actuals within 95% confidence interval

CALL gold_sales_forecast_model!SHOW_EVALUATION_METRICS();

-- ============================================================================
-- Step 4: Generate Forecast (90 days)
-- ============================================================================
-- Output columns:
--   TS          -> Forecast date
--   FORECAST    -> Predicted value
--   LOWER_BOUND -> Lower bound of 95% confidence interval
--   UPPER_BOUND -> Upper bound of 95% confidence interval

CREATE OR REPLACE TABLE &{SCHEMA}.gold_forecast_revenue_90days AS
SELECT 
    TS::DATE AS forecast_date,
    ROUND(FORECAST, 2) AS forecasted_revenue,
    ROUND(LOWER_BOUND, 2) AS lower_bound_95pct,
    ROUND(UPPER_BOUND, 2) AS upper_bound_95pct,
    ROUND(UPPER_BOUND - LOWER_BOUND, 2) AS confidence_interval_width,
    CURRENT_TIMESTAMP() AS forecast_generated_at
FROM TABLE(
    gold_sales_forecast_model!FORECAST(
        FORECASTING_PERIODS => 90
    )
)
ORDER BY forecast_date;

COMMENT ON TABLE &{SCHEMA}.gold_forecast_revenue_90days IS '90-day revenue forecast with 95% confidence intervals';

-- ============================================================================
-- Step 5: Extended Forecast (180 days)
-- ============================================================================
-- Useful for longer-term planning and budgeting

CREATE OR REPLACE TABLE &{SCHEMA}.gold_forecast_revenue_180days AS
SELECT 
    TS::DATE AS forecast_date,
    ROUND(FORECAST, 2) AS forecasted_revenue,
    ROUND(LOWER_BOUND, 2) AS lower_bound_95pct,
    ROUND(UPPER_BOUND, 2) AS upper_bound_95pct,
    ROUND(UPPER_BOUND - LOWER_BOUND, 2) AS confidence_interval_width,
    CURRENT_TIMESTAMP() AS forecast_generated_at
FROM TABLE(
    gold_sales_forecast_model!FORECAST(
        FORECASTING_PERIODS => 180
    )
)
ORDER BY forecast_date;

COMMENT ON TABLE &{SCHEMA}.gold_forecast_revenue_180days IS '180-day revenue forecast with 95% confidence intervals';

-- ============================================================================
-- Step 6: Combined View (Actuals + Forecast for 90 days)
-- ============================================================================
-- Single view showing historical data and predictions side by side
-- Common pattern for dashboards and reporting

CREATE OR REPLACE VIEW &{SCHEMA}.gold_vw_actuals_vs_forecast_90days AS

-- Historical actual values
SELECT
    sale_date AS date,
    daily_revenue AS actual_revenue,
    NULL::DECIMAL(10,2) AS forecast_revenue,
    NULL::DECIMAL(10,2) AS lower_bound,
    NULL::DECIMAL(10,2) AS upper_bound,
    'ACTUAL' AS data_type,
    NULL::INTEGER AS days_ahead
FROM &{SCHEMA}.silver_daily_sales

UNION ALL

-- Forecast predictions
SELECT
    forecast_date AS date,
    NULL::DECIMAL(10,2) AS actual_revenue,
    forecasted_revenue AS forecast_revenue,
    lower_bound_95pct AS lower_bound,
    upper_bound_95pct AS upper_bound,
    'FORECAST' AS data_type,
    DATEDIFF(DAY, CURRENT_DATE(), forecast_date)::INTEGER AS days_ahead
FROM &{SCHEMA}.gold_forecast_revenue_90days

ORDER BY date;

COMMENT ON VIEW &{SCHEMA}.gold_vw_actuals_vs_forecast_90days IS 'Combined view of 2 years actuals + 90-day forecast for dashboards';

-- ============================================================================
-- Step 7: Extended Combined View (Actuals + 180 days Forecast)
-- ============================================================================

CREATE OR REPLACE VIEW &{SCHEMA}.gold_vw_actuals_vs_forecast_180days AS

-- Historical actual values
SELECT
    sale_date AS date,
    daily_revenue AS actual_revenue,
    NULL::DECIMAL(10,2) AS forecast_revenue,
    NULL::DECIMAL(10,2) AS lower_bound,
    NULL::DECIMAL(10,2) AS upper_bound,
    'ACTUAL' AS data_type,
    NULL::INTEGER AS days_ahead
FROM &{SCHEMA}.silver_daily_sales

UNION ALL

-- Forecast predictions
SELECT
    forecast_date AS date,
    NULL::DECIMAL(10,2) AS actual_revenue,
    forecasted_revenue AS forecast_revenue,
    lower_bound_95pct AS lower_bound,
    upper_bound_95pct AS upper_bound,
    'FORECAST' AS data_type,
    DATEDIFF(DAY, CURRENT_DATE(), forecast_date)::INTEGER AS days_ahead
FROM &{SCHEMA}.gold_forecast_revenue_180days

ORDER BY date;

COMMENT ON VIEW &{SCHEMA}.gold_vw_actuals_vs_forecast_180days IS 'Combined view of 2 years actuals + 180-day forecast for long-term planning';

-- ============================================================================
-- Step 8: Monthly Forecast Summary
-- ============================================================================
-- Aggregated forecast by month for business review

CREATE OR REPLACE VIEW &{SCHEMA}.gold_vw_forecast_monthly_summary AS
SELECT
    DATE_TRUNC('MONTH', forecast_date)::DATE AS month_date,
    EXTRACT(YEAR FROM forecast_date)::INTEGER AS year,
    EXTRACT(MONTH FROM forecast_date)::INTEGER AS month,
    COUNT(*) AS forecast_days,
    ROUND(SUM(forecasted_revenue), 2) AS total_forecasted_revenue,
    ROUND(AVG(forecasted_revenue), 2) AS avg_daily_forecast,
    ROUND(MIN(forecasted_revenue), 2) AS min_daily_forecast,
    ROUND(MAX(forecasted_revenue), 2) AS max_daily_forecast,
    ROUND(AVG(lower_bound_95pct), 2) AS avg_lower_bound,
    ROUND(AVG(upper_bound_95pct), 2) AS avg_upper_bound
FROM &{SCHEMA}.gold_forecast_revenue_90days
GROUP BY 1, 2, 3
ORDER BY month_date;

COMMENT ON VIEW &{SCHEMA}.gold_vw_forecast_monthly_summary IS 'Monthly aggregated revenue forecast for business planning';

-- ============================================================================
-- Step 9: Forecast Quality Assessment
-- ============================================================================
-- Compare recent forecast accuracy

SELECT 
    'Forecast Model Performance' AS assessment,
    COUNT(*) AS forecast_records,
    ROUND(AVG(confidence_interval_width), 2) AS avg_confidence_width,
    ROUND(SUM(forecasted_revenue), 2) AS total_90day_forecast,
    ROUND(AVG(forecasted_revenue), 2) AS avg_daily_forecast,
    CURRENT_TIMESTAMP() AS assessment_date
FROM &{SCHEMA}.gold_forecast_revenue_90days;

-- ============================================================================
-- Step 10: Query Examples
-- ============================================================================

-- 10a. View transition from actuals to forecast
SELECT * FROM &{SCHEMA}.gold_vw_actuals_vs_forecast_90days
WHERE date >= DATEADD(DAY, -15, CURRENT_DATE())
ORDER BY date;

-- 10b. Month-by-month forecast breakdown
SELECT 
    month_date,
    year,
    month,
    total_forecasted_revenue,
    avg_daily_forecast,
    forecast_days
FROM &{SCHEMA}.gold_vw_forecast_monthly_summary
ORDER BY month_date;

-- 10c. Forecast uncertainty (confidence interval width)
SELECT 
    forecast_date,
    forecasted_revenue,
    lower_bound_95pct,
    upper_bound_95pct,
    confidence_interval_width,
    ROUND(
        confidence_interval_width / NULLIF(forecasted_revenue, 0) * 100, 2
    ) AS uncertainty_pct
FROM &{SCHEMA}.gold_forecast_revenue_90days
WHERE forecast_date <= DATEADD(DAY, 30, CURRENT_DATE())
ORDER BY forecast_date;
