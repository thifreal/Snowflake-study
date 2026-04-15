-- SILVER LAYER: Data Cleaning and Standardization
-- Purpose: Clean, deduplicate, and standardize data from bronze layer
-- Pattern: Apply business rules, quality checks, and transformations
--
-- Key Principles:
-- 1. Remove duplicates and nulls (based on business rules)
-- 2. Standardize data types and formats
-- 3. Apply business rules and validations
-- 4. Add lineage and quality indicators
-- 5. Maintain SCD Type 2 when applicable

-- ============================================================================
-- TEMPLATE: Create Silver Dimension Table
-- ============================================================================
-- Description: Cleaned and standardized dimension table with SCD Type 2

CREATE OR REPLACE TABLE &{SCHEMA}.silver_customer (
    -- Business key
    customer_id STRING NOT NULL,
    
    -- Business attributes
    customer_name STRING NOT NULL,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    postal_code STRING,
    country STRING,
    
    -- Data quality flags
    is_valid_email BOOLEAN,
    is_active BOOLEAN DEFAULT TRUE,
    
    -- SCD Type 2 columns
    effective_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    _created_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_table STRING,
    _dbt_version STRING,
    
    PRIMARY KEY (customer_id, effective_date)
)
COMMENT = 'Silver layer: Cleaned customer dimension';

-- ============================================================================
-- TEMPLATE: Deduplication and Cleaning
-- ============================================================================

WITH deduplicated_data AS (
    SELECT 
        TRIM(UPPER(id)) as customer_id,
        TRIM(name) as customer_name,
        LOWER(TRIM(email)) as email,
        PHONE,
        ADDRESS,
        CITY,
        STATE,
        POSTAL_CODE,
        COUNTRY,
        CASE 
            WHEN email LIKE '%@%.%' THEN TRUE 
            ELSE FALSE 
        END as is_valid_email,
        _load_date,
        ROW_NUMBER() OVER (PARTITION BY TRIM(UPPER(id)) ORDER BY _load_date DESC) as rn
    FROM &{SOURCE_SCHEMA}.bronze_data
    WHERE id IS NOT NULL
)
SELECT 
    customer_id,
    customer_name,
    email,
    phone,
    address,
    city,
    state,
    postal_code,
    country,
    is_valid_email,
    TRUE as is_active,
    CURRENT_DATE() as effective_date,
    NULL as end_date,
    TRUE as is_current,
    CURRENT_TIMESTAMP() as _created_date,
    CURRENT_TIMESTAMP() as _updated_date,
    'bronze_data' as _source_table,
    '1.0' as _dbt_version
FROM deduplicated_data
WHERE rn = 1  -- Keep only latest version of each customer
;

-- ============================================================================
-- TEMPLATE: Data Quality Checks for Silver
-- ============================================================================

WITH quality_checks AS (
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN customer_id IS NULL THEN 1 END) as null_ids,
        COUNT(CASE WHEN customer_name IS NULL THEN 1 END) as null_names,
        COUNT(CASE WHEN is_valid_email = FALSE THEN 1 END) as invalid_emails,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM &{SCHEMA}.silver_customer
    WHERE is_current = TRUE
)
SELECT 
    *,
    CASE 
        WHEN null_ids = 0 AND null_names = 0 AND invalid_emails < (total_records * 0.05) 
        THEN 'PASSED'
        ELSE 'FAILED'
    END as quality_status
FROM quality_checks
;

-- ============================================================================
-- TEMPLATE: Handle SCD Type 2 Updates
-- ============================================================================

-- When updating dimensions, expire old records
UPDATE &{SCHEMA}.silver_customer
SET 
    end_date = DATEADD(DAY, -1, CURRENT_DATE()),
    is_current = FALSE
WHERE 
    is_current = TRUE
    AND customer_id IN (
        -- Find customers with changes
        SELECT DISTINCT customer_id 
        FROM &{SOURCE_SCHEMA}.bronze_data
        WHERE _load_date >= DATEADD(DAY, -1, CURRENT_DATE())
    )
;

-- Insert new versions
INSERT INTO &{SCHEMA}.silver_customer
SELECT 
    customer_id,
    customer_name,
    email,
    phone,
    address,
    city,
    state,
    postal_code,
    country,
    is_valid_email,
    is_active,
    CURRENT_DATE() as effective_date,
    NULL as end_date,
    TRUE as is_current,
    CURRENT_TIMESTAMP() as _created_date,
    CURRENT_TIMESTAMP() as _updated_date,
    'bronze_data' as _source_table,
    '1.0' as _dbt_version
FROM &{SCHEMA}.deduplicated_data
WHERE rn = 1
;
