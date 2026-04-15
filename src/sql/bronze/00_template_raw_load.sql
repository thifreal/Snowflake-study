-- BRONZE LAYER: Raw Data Ingestion
-- Purpose: Load raw data from source systems with minimal transformation
-- Pattern: Create staging tables, load data, then swap into bronze tables
-- 
-- Key Principles:
-- 1. Maintain data structure as close to source as possible
-- 2. Add metadata columns (load_date, source_system)
-- 3. Keep full historical data
-- 4. Minimal data type conversions

-- ============================================================================
-- TEMPLATE: Create Bronze Staging Table
-- ============================================================================
-- Description: Staging table for raw data before moving to bronze layer

CREATE OR REPLACE TABLE &{SCHEMA}.stg_raw_data (
    -- Business columns
    id STRING,
    name STRING,
    email STRING,
    created_date TIMESTAMP_NTZ,
    updated_date TIMESTAMP_NTZ,
    
    -- Metadata columns
    _load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_system STRING,
    _file_name STRING,
    _raw_data VARIANT  -- Optional: store raw JSON if needed
)
COMMENT = 'Staging table for raw data ingestion';

-- ============================================================================
-- TEMPLATE: Load Data into Bronze
-- ============================================================================
-- Description: Load from staging to bronze table with data validation

INSERT INTO &{SCHEMA}.bronze_data (
    id, name, email, created_date, updated_date, _load_date, _source_system
)
SELECT 
    id,
    name,
    email,
    created_date,
    updated_date,
    CURRENT_TIMESTAMP() as _load_date,
    'SOURCE_SYSTEM_NAME' as _source_system
FROM &{SCHEMA}.stg_raw_data
WHERE id IS NOT NULL  -- Basic validation
;

-- ============================================================================
-- TEMPLATE: Add Data Quality Checks
-- ============================================================================

-- Check for duplicates
SELECT id, COUNT(*) 
FROM &{SCHEMA}.bronze_data 
GROUP BY id 
HAVING COUNT(*) > 1;

-- Check null values
SELECT 
    COUNT(CASE WHEN id IS NULL THEN 1 END) as null_ids,
    COUNT(CASE WHEN name IS NULL THEN 1 END) as null_names
FROM &{SCHEMA}.bronze_data;

-- ============================================================================
-- TEMPLATE: Cleanup and Archive
-- ============================================================================

-- Archive old data (optional)
CREATE TABLE IF NOT EXISTS &{SCHEMA}.bronze_data_archive AS
SELECT * FROM &{SCHEMA}.bronze_data
WHERE _load_date < DATEADD(DAY, -30, CURRENT_DATE());

-- Clear staging table
TRUNCATE TABLE &{SCHEMA}.stg_raw_data;
