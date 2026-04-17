-- ============================================================================
-- SNOWFLAKE LEARNING GUIDE - EXPANDED WITH EXAMPLES
-- ============================================================================
-- Author:  Thiago F. A. Almeida
-- Date:    April 16, 2026
--
-- Covers 3 levels of Snowflake skills with runnable SQL examples.
-- Run each section in a Snowflake worksheet to learn by doing.
--
-- BOOKMARK THESE:
--   docs.snowflake.com          - Full documentation
--   quickstarts.snowflake.com   - Hands-on guided tutorials
--   youtube.com/@SnowflakeInc   - Video demos and talks
--   community.snowflake.com     - Q and A forum
--   learn.snowflake.com         - Free courses and certifications
-- ============================================================================


-- ############################################################################
--   LEVEL 1: SQL FUNDAMENTALS
-- ############################################################################

-- ============================================================================
-- 1.1  SELECT / WHERE / GROUP BY / ORDER BY / HAVING
-- ============================================================================
-- Docs: docs.snowflake.com/en/sql-reference/constructs

USE DATABASE SALES_FORECAST_LAB;
USE SCHEMA PRACTICE;

-- ---------------------------------------------------------------------------
-- SELECT basics: pick columns, rename with AS, compute expressions
-- ---------------------------------------------------------------------------

-- Basic SELECT with filter
SELECT sale_date, daily_revenue, num_orders
FROM DAILY_SALES
WHERE daily_revenue > 200
  AND sale_date >= '2024-01-01'
ORDER BY daily_revenue DESC
LIMIT 10;

-- Computed columns and aliases
SELECT
    sale_date,
    daily_revenue,
    num_orders,
    ROUND(daily_revenue / NULLIF(num_orders, 0), 2) AS revenue_per_order,
    daily_revenue * 0.9                              AS revenue_after_10pct_tax
FROM DAILY_SALES
WHERE sale_date >= '2024-06-01'
ORDER BY sale_date
LIMIT 10;

-- DISTINCT: remove duplicate values
SELECT DISTINCT
    DATE_TRUNC('MONTH', sale_date)::DATE AS month
FROM DAILY_SALES
ORDER BY month;

-- ---------------------------------------------------------------------------
-- WHERE: filter rows BEFORE grouping
-- ---------------------------------------------------------------------------

-- OR / AND precedence (use parentheses to be explicit)
SELECT sale_date, daily_revenue, num_orders
FROM DAILY_SALES
WHERE (daily_revenue > 300 OR num_orders > 20)
  AND sale_date >= '2024-01-01'
ORDER BY sale_date
LIMIT 10;

-- IN: match against a list of values
SELECT sale_date, daily_revenue
FROM DAILY_SALES
WHERE DAYNAME(sale_date) IN ('Mon', 'Fri')
  AND sale_date BETWEEN '2024-06-01' AND '2024-06-30'
ORDER BY sale_date;

-- LIKE: pattern matching (% = any chars, _ = one char)
-- Useful on string columns; here we demo with a date cast
SELECT sale_date, daily_revenue
FROM DAILY_SALES
WHERE TO_VARCHAR(sale_date, 'YYYY-MM') LIKE '2024-06'
ORDER BY sale_date;

-- BETWEEN: inclusive range filter
SELECT sale_date, daily_revenue
FROM DAILY_SALES
WHERE daily_revenue BETWEEN 100 AND 200
  AND sale_date >= '2024-01-01'
ORDER BY daily_revenue DESC
LIMIT 10;

-- IS NULL / IS NOT NULL
-- (DAILY_SALES likely has no NULLs, but this pattern is essential)
SELECT sale_date, daily_revenue
FROM DAILY_SALES
WHERE daily_revenue IS NOT NULL
LIMIT 5;

-- NOT: negate a condition
SELECT sale_date, daily_revenue
FROM DAILY_SALES
WHERE NOT DAYNAME(sale_date) IN ('Sat', 'Sun')   -- weekdays only
  AND sale_date BETWEEN '2024-06-01' AND '2024-06-30'
ORDER BY sale_date;

-- ---------------------------------------------------------------------------
-- GROUP BY: collapse rows into groups and aggregate
-- ---------------------------------------------------------------------------

-- GROUP BY with aggregation
SELECT
    DATE_TRUNC('MONTH', sale_date)::DATE AS month,
    SUM(daily_revenue)  AS total_revenue,
    AVG(daily_revenue)  AS avg_revenue,
    COUNT(*)            AS days_in_month
FROM DAILY_SALES
GROUP BY month
ORDER BY month;

-- Group by day-of-week: which weekday sells the most?
SELECT
    DAYNAME(sale_date)        AS weekday,
    DAYOFWEEK(sale_date)      AS weekday_num,   -- for sorting Mon-Sun
    COUNT(*)                  AS num_days,
    ROUND(AVG(daily_revenue), 2) AS avg_revenue,
    SUM(daily_revenue)        AS total_revenue
FROM DAILY_SALES
WHERE sale_date >= '2024-01-01'
GROUP BY weekday, weekday_num
ORDER BY weekday_num;

-- Multiple aggregates: MIN, MAX, MEDIAN, STDDEV
SELECT
    DATE_TRUNC('QUARTER', sale_date)::DATE AS quarter,
    COUNT(*)                   AS days,
    ROUND(MIN(daily_revenue), 2)   AS min_rev,
    ROUND(MAX(daily_revenue), 2)   AS max_rev,
    ROUND(MEDIAN(daily_revenue), 2) AS median_rev,
    ROUND(STDDEV(daily_revenue), 2) AS stddev_rev
FROM DAILY_SALES
GROUP BY quarter
ORDER BY quarter;

-- GROUP BY with CASE: bucket revenue into tiers then count
SELECT
    CASE
        WHEN daily_revenue >= 250 THEN 'HIGH (250+)'
        WHEN daily_revenue >= 150 THEN 'MEDIUM (150-249)'
        WHEN daily_revenue >= 100 THEN 'LOW (100-149)'
        ELSE 'VERY LOW (<100)'
    END AS revenue_tier,
    COUNT(*) AS num_days,
    ROUND(AVG(daily_revenue), 2) AS avg_rev_in_tier
FROM DAILY_SALES
GROUP BY revenue_tier
ORDER BY avg_rev_in_tier DESC;

-- ---------------------------------------------------------------------------
-- ORDER BY: sort results (ASC default, DESC explicit)
-- ---------------------------------------------------------------------------

-- Multi-column sort: month ascending, then revenue descending within month
SELECT
    DATE_TRUNC('MONTH', sale_date)::DATE AS month,
    sale_date,
    daily_revenue
FROM DAILY_SALES
WHERE sale_date >= '2024-01-01'
ORDER BY month ASC, daily_revenue DESC
LIMIT 20;

-- Order by column position (less readable but sometimes handy)
SELECT sale_date, daily_revenue, num_orders
FROM DAILY_SALES
ORDER BY 2 DESC   -- 2 = second column (daily_revenue)
LIMIT 5;

-- NULLS FIRST / NULLS LAST (controls where NULLs appear in sort)
SELECT sale_date, daily_revenue
FROM DAILY_SALES
ORDER BY daily_revenue DESC NULLS LAST
LIMIT 10;

-- ---------------------------------------------------------------------------
-- HAVING: filter AFTER grouping (works on aggregate results)
-- ---------------------------------------------------------------------------

-- HAVING: filter after grouping
SELECT
    DATE_TRUNC('MONTH', sale_date)::DATE AS month,
    SUM(daily_revenue) AS total_revenue
FROM DAILY_SALES
GROUP BY month
HAVING SUM(daily_revenue) > 5000
ORDER BY total_revenue DESC;

-- HAVING with COUNT: only months with more than 28 selling days
SELECT
    DATE_TRUNC('MONTH', sale_date)::DATE AS month,
    COUNT(*) AS selling_days,
    SUM(daily_revenue) AS total_revenue
FROM DAILY_SALES
GROUP BY month
HAVING COUNT(*) > 28
ORDER BY month;

-- HAVING with AVG: months where average daily revenue exceeded 200
SELECT
    DATE_TRUNC('MONTH', sale_date)::DATE AS month,
    ROUND(AVG(daily_revenue), 2) AS avg_daily_revenue,
    SUM(daily_revenue) AS total_revenue
FROM DAILY_SALES
GROUP BY month
HAVING AVG(daily_revenue) > 200
ORDER BY avg_daily_revenue DESC;

-- ---------------------------------------------------------------------------
-- Combining everything: WHERE + GROUP BY + HAVING + ORDER BY
-- "For weekdays in 2024, which months averaged > 180 revenue per day?"
-- ---------------------------------------------------------------------------
SELECT
    DATE_TRUNC('MONTH', sale_date)::DATE AS month,
    COUNT(*)                             AS weekday_count,
    ROUND(AVG(daily_revenue), 2)         AS avg_weekday_revenue
FROM DAILY_SALES
WHERE sale_date >= '2024-01-01'                   -- WHERE filters rows first
  AND DAYNAME(sale_date) NOT IN ('Sat', 'Sun')    -- weekdays only
GROUP BY month                                     -- then group
HAVING AVG(daily_revenue) > 180                    -- then filter groups
ORDER BY avg_weekday_revenue DESC;                 -- then sort


-- ============================================================================
-- 1.2  JOINs
-- ============================================================================
-- Docs: docs.snowflake.com/en/sql-reference/constructs/join

-- ---------------------------------------------------------------------------
-- Setup: reference tables for JOIN examples
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE PRODUCT_CATEGORIES (
    category_id INT, category_name VARCHAR(50), margin_pct FLOAT
);
INSERT INTO PRODUCT_CATEGORIES VALUES
    (1,'Electronics',0.15),(2,'Clothing',0.40),(3,'Food',0.25),
    (4,'Home',0.35),(5,'Books',0.50);

CREATE OR REPLACE TABLE SALES_BY_CATEGORY AS
SELECT d.sale_date, c.category_id,
    ROUND(d.daily_revenue * UNIFORM(0.1,0.4,RANDOM()),2) AS category_revenue
FROM DAILY_SALES d CROSS JOIN PRODUCT_CATEGORIES c
WHERE d.sale_date >= '2024-01-01';

CREATE OR REPLACE TABLE MYSTERY_SALES (sale_date DATE, category_id INT, amount FLOAT);
INSERT INTO MYSTERY_SALES VALUES
    ('2024-01-01',1,100),('2024-01-01',99,50),('2024-01-02',3,75);

-- ---------------------------------------------------------------------------
-- INNER JOIN: only rows that match in BOTH tables
-- ---------------------------------------------------------------------------

SELECT s.sale_date, p.category_name, s.category_revenue,
    ROUND(s.category_revenue * p.margin_pct, 2) AS estimated_profit
FROM SALES_BY_CATEGORY s
INNER JOIN PRODUCT_CATEGORIES p ON s.category_id = p.category_id
WHERE s.sale_date = '2024-06-15'
ORDER BY estimated_profit DESC;

-- INNER JOIN with multiple conditions
SELECT s.sale_date, p.category_name, s.category_revenue
FROM SALES_BY_CATEGORY s
INNER JOIN PRODUCT_CATEGORIES p
    ON s.category_id = p.category_id
    AND p.margin_pct >= 0.30          -- only high-margin categories
WHERE s.sale_date = '2024-06-15'
ORDER BY s.category_revenue DESC;

-- ---------------------------------------------------------------------------
-- LEFT JOIN: all rows from left table, NULLs where no match on right
-- ---------------------------------------------------------------------------

-- Find orphan records (category_id=99 has no match)
SELECT m.sale_date, m.category_id, m.amount, p.category_name
FROM MYSTERY_SALES m
LEFT JOIN PRODUCT_CATEGORIES p ON m.category_id = p.category_id;

-- LEFT JOIN + IS NULL: find rows that DON'T have a match (anti-join pattern)
SELECT m.*
FROM MYSTERY_SALES m
LEFT JOIN PRODUCT_CATEGORIES p ON m.category_id = p.category_id
WHERE p.category_id IS NULL;   -- only orphans

-- ---------------------------------------------------------------------------
-- RIGHT JOIN: all rows from right table, NULLs where no match on left
-- (mirror of LEFT JOIN - less common, but good to know)
-- ---------------------------------------------------------------------------

SELECT p.category_name, m.sale_date, m.amount
FROM MYSTERY_SALES m
RIGHT JOIN PRODUCT_CATEGORIES p ON m.category_id = p.category_id
ORDER BY p.category_name;
-- Categories with no mystery sales show NULL for sale_date and amount

-- ---------------------------------------------------------------------------
-- FULL OUTER JOIN: all rows from both sides, NULLs where no match
-- ---------------------------------------------------------------------------

SELECT
    COALESCE(m.category_id, p.category_id) AS cat_id,
    p.category_name,
    m.sale_date,
    m.amount
FROM MYSTERY_SALES m
FULL OUTER JOIN PRODUCT_CATEGORIES p ON m.category_id = p.category_id
ORDER BY cat_id;
-- Shows both: orphan sales (cat 99) AND categories with no mystery sales

-- ---------------------------------------------------------------------------
-- CROSS JOIN: every row from A paired with every row from B (cartesian)
-- Useful for generating combinations or date spines
-- ---------------------------------------------------------------------------

-- Generate a date-category grid (every date x every category)
SELECT d.sale_date, p.category_name
FROM (SELECT DISTINCT sale_date FROM DAILY_SALES
      WHERE sale_date BETWEEN '2024-06-01' AND '2024-06-03') d
CROSS JOIN PRODUCT_CATEGORIES p
ORDER BY d.sale_date, p.category_name;

-- ---------------------------------------------------------------------------
-- Self JOIN: join a table to itself (compare rows within same table)
-- ---------------------------------------------------------------------------

-- Compare each day's revenue to the previous day
SELECT
    t.sale_date       AS today,
    t.daily_revenue   AS today_rev,
    y.sale_date       AS yesterday,
    y.daily_revenue   AS yesterday_rev,
    ROUND(t.daily_revenue - y.daily_revenue, 2) AS day_over_day
FROM DAILY_SALES t
INNER JOIN DAILY_SALES y ON t.sale_date = DATEADD(DAY, 1, y.sale_date)
WHERE t.sale_date BETWEEN '2024-06-01' AND '2024-06-10'
ORDER BY t.sale_date;

-- ---------------------------------------------------------------------------
-- Multi-table JOIN: chaining 3+ tables together
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE REGIONS (region_id INT, region_name VARCHAR(30));
INSERT INTO REGIONS VALUES (1,'North'),(2,'South'),(3,'East'),(4,'West');

CREATE OR REPLACE TABLE CATEGORY_REGIONS (category_id INT, region_id INT, weight FLOAT);
INSERT INTO CATEGORY_REGIONS VALUES
    (1,1,0.4),(1,2,0.3),(1,3,0.2),(1,4,0.1),
    (2,1,0.25),(2,2,0.25),(2,3,0.25),(2,4,0.25);

SELECT
    p.category_name,
    r.region_name,
    cr.weight,
    ROUND(s.category_revenue * cr.weight, 2) AS regional_revenue
FROM SALES_BY_CATEGORY s
INNER JOIN PRODUCT_CATEGORIES p  ON s.category_id = p.category_id
INNER JOIN CATEGORY_REGIONS cr   ON s.category_id = cr.category_id
INNER JOIN REGIONS r             ON cr.region_id  = r.region_id
WHERE s.sale_date = '2024-06-15'
  AND p.category_name = 'Electronics'
ORDER BY regional_revenue DESC;

-- ---------------------------------------------------------------------------
-- JOIN with aggregation: total revenue per category
-- ---------------------------------------------------------------------------

SELECT
    p.category_name,
    COUNT(*)                         AS num_records,
    ROUND(SUM(s.category_revenue), 2) AS total_revenue,
    ROUND(AVG(s.category_revenue), 2) AS avg_revenue
FROM SALES_BY_CATEGORY s
INNER JOIN PRODUCT_CATEGORIES p ON s.category_id = p.category_id
WHERE s.sale_date BETWEEN '2024-06-01' AND '2024-06-30'
GROUP BY p.category_name
ORDER BY total_revenue DESC;


-- ============================================================================
-- 1.3  CTEs (Common Table Expressions)
-- ============================================================================
-- Docs: docs.snowflake.com/en/sql-reference/constructs/with

-- ---------------------------------------------------------------------------
-- Basic CTE: name a subquery, reference it below
-- ---------------------------------------------------------------------------

WITH monthly_totals AS (
    SELECT DATE_TRUNC('MONTH', sale_date)::DATE AS month,
        SUM(daily_revenue) AS revenue
    FROM DAILY_SALES GROUP BY month
),
overall_avg AS (
    SELECT AVG(revenue) AS avg_monthly FROM monthly_totals
)
SELECT m.month, ROUND(m.revenue,2) AS revenue,
    ROUND(o.avg_monthly,2) AS avg_revenue,
    ROUND(m.revenue - o.avg_monthly,2) AS diff_from_avg
FROM monthly_totals m CROSS JOIN overall_avg o
ORDER BY m.revenue DESC LIMIT 5;

-- ---------------------------------------------------------------------------
-- Chained CTEs: each CTE can reference the ones defined before it
-- "Monthly revenue -> quarter summary -> rank quarters"
-- ---------------------------------------------------------------------------

WITH monthly AS (
    SELECT DATE_TRUNC('MONTH', sale_date)::DATE AS month,
        SUM(daily_revenue) AS revenue
    FROM DAILY_SALES
    WHERE sale_date >= '2024-01-01'
    GROUP BY month
),
quarterly AS (
    SELECT DATE_TRUNC('QUARTER', month)::DATE AS quarter,
        SUM(revenue) AS quarterly_revenue,
        COUNT(*)     AS months_in_quarter
    FROM monthly
    GROUP BY quarter
),
ranked AS (
    SELECT quarter, quarterly_revenue, months_in_quarter,
        RANK() OVER (ORDER BY quarterly_revenue DESC) AS revenue_rank
    FROM quarterly
)
SELECT * FROM ranked ORDER BY revenue_rank;

-- ---------------------------------------------------------------------------
-- CTE for de-duplication: keep latest record per group
-- ---------------------------------------------------------------------------

-- Simulate a table with duplicates
CREATE OR REPLACE TEMP TABLE RAW_EVENTS (
    event_id INT, user_name VARCHAR(30), event_time TIMESTAMP, score INT
);
INSERT INTO RAW_EVENTS VALUES
    (1,'alice','2024-06-01 10:00',80),
    (2,'alice','2024-06-01 14:00',95),  -- alice's latest
    (3,'bob',  '2024-06-01 09:00',70),
    (4,'bob',  '2024-06-01 11:00',88),
    (5,'bob',  '2024-06-01 16:00',92);  -- bob's latest

WITH ranked_events AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY user_name ORDER BY event_time DESC) AS rn
    FROM RAW_EVENTS
)
SELECT event_id, user_name, event_time, score
FROM ranked_events
WHERE rn = 1;   -- only the latest event per user

-- ---------------------------------------------------------------------------
-- CTE with aggregation at multiple granularities
-- "Show each day's revenue plus its monthly and overall average side by side"
-- ---------------------------------------------------------------------------

WITH daily AS (
    SELECT sale_date, daily_revenue
    FROM DAILY_SALES
    WHERE sale_date BETWEEN '2024-06-01' AND '2024-06-30'
),
monthly_avg AS (
    SELECT ROUND(AVG(daily_revenue), 2) AS month_avg FROM daily
),
weekly_avg AS (
    SELECT DATE_TRUNC('WEEK', sale_date)::DATE AS week_start,
        ROUND(AVG(daily_revenue), 2) AS week_avg
    FROM daily GROUP BY week_start
)
SELECT
    d.sale_date,
    d.daily_revenue,
    w.week_avg,
    m.month_avg,
    ROUND(d.daily_revenue - m.month_avg, 2) AS diff_from_month
FROM daily d
LEFT JOIN weekly_avg w ON DATE_TRUNC('WEEK', d.sale_date)::DATE = w.week_start
CROSS JOIN monthly_avg m
ORDER BY d.sale_date;

-- ---------------------------------------------------------------------------
-- Recursive CTE: generate a series (e.g., date spine)
-- Recursive CTEs reference themselves to build row-by-row
-- ---------------------------------------------------------------------------

WITH RECURSIVE date_spine (dt) AS (
    -- Anchor: starting date
    SELECT '2024-06-01'::DATE AS dt
    UNION ALL
    -- Recursive: add 1 day until we reach the end
    SELECT DATEADD(DAY, 1, dt)
    FROM date_spine
    WHERE dt < '2024-06-30'
)
SELECT ds.dt AS calendar_date,
    COALESCE(d.daily_revenue, 0) AS revenue,
    CASE WHEN d.sale_date IS NULL THEN 'NO SALES' ELSE 'HAS SALES' END AS status
FROM date_spine ds
LEFT JOIN DAILY_SALES d ON ds.dt = d.sale_date
ORDER BY ds.dt;

-- Recursive CTE: build an org hierarchy
CREATE OR REPLACE TEMP TABLE EMPLOYEES (
    emp_id INT, emp_name VARCHAR(30), manager_id INT
);
INSERT INTO EMPLOYEES VALUES
    (1,'CEO',NULL),(2,'VP Sales',1),(3,'VP Eng',1),
    (4,'Sales Mgr',2),(5,'Engineer',3),(6,'Sales Rep',4);

WITH RECURSIVE org_tree AS (
    -- Anchor: top-level (no manager)
    SELECT emp_id, emp_name, manager_id, 0 AS depth,
        emp_name::VARCHAR(200) AS path
    FROM EMPLOYEES WHERE manager_id IS NULL
    UNION ALL
    -- Recursive: find direct reports
    SELECT e.emp_id, e.emp_name, e.manager_id, o.depth + 1,
        (o.path || ' > ' || e.emp_name)::VARCHAR(200)
    FROM EMPLOYEES e
    INNER JOIN org_tree o ON e.manager_id = o.emp_id
)
SELECT REPEAT('  ', depth) || emp_name AS org_chart, depth, path
FROM org_tree
ORDER BY path;


-- ============================================================================
-- 1.4  Window Functions (ROW_NUMBER, LAG, LEAD, SUM OVER)
-- ============================================================================
-- Docs: docs.snowflake.com/en/sql-reference/functions-analytic

-- ---------------------------------------------------------------------------
-- ROW_NUMBER: sequential number within each partition (no ties)
-- ---------------------------------------------------------------------------

-- Top 3 revenue days per month
SELECT sale_date, daily_revenue,
    DATE_TRUNC('MONTH', sale_date)::DATE AS month,
    ROW_NUMBER() OVER (
        PARTITION BY DATE_TRUNC('MONTH', sale_date)
        ORDER BY daily_revenue DESC
    ) AS rank_in_month
FROM DAILY_SALES
QUALIFY rank_in_month <= 3
ORDER BY month, rank_in_month LIMIT 15;

-- ---------------------------------------------------------------------------
-- RANK vs DENSE_RANK: handling ties
-- RANK:       1, 2, 2, 4  (skips after tie)
-- DENSE_RANK: 1, 2, 2, 3  (no skip)
-- ---------------------------------------------------------------------------

SELECT sale_date, daily_revenue,
    RANK()       OVER (ORDER BY daily_revenue DESC) AS rank_val,
    DENSE_RANK() OVER (ORDER BY daily_revenue DESC) AS dense_rank_val
FROM DAILY_SALES
WHERE sale_date BETWEEN '2024-06-01' AND '2024-06-15'
ORDER BY rank_val
LIMIT 15;

-- ---------------------------------------------------------------------------
-- NTILE: divide rows into N roughly-equal buckets
-- ---------------------------------------------------------------------------

-- Split June into 4 quartile buckets by revenue
SELECT sale_date, daily_revenue,
    NTILE(4) OVER (ORDER BY daily_revenue) AS quartile
FROM DAILY_SALES
WHERE sale_date BETWEEN '2024-06-01' AND '2024-06-30'
ORDER BY quartile, daily_revenue;

-- ---------------------------------------------------------------------------
-- PERCENT_RANK and CUME_DIST: relative position within the window
-- PERCENT_RANK = (rank - 1) / (total - 1)   ->  0.0 to 1.0
-- CUME_DIST    = rows <= current / total     ->  > 0.0 to 1.0
-- ---------------------------------------------------------------------------

SELECT sale_date, daily_revenue,
    ROUND(PERCENT_RANK() OVER (ORDER BY daily_revenue), 4) AS pct_rank,
    ROUND(CUME_DIST()    OVER (ORDER BY daily_revenue), 4) AS cume_dist
FROM DAILY_SALES
WHERE sale_date BETWEEN '2024-06-01' AND '2024-06-30'
ORDER BY daily_revenue;

-- ---------------------------------------------------------------------------
-- LAG / LEAD: look back / look ahead in the ordered set
-- ---------------------------------------------------------------------------

-- Compare to previous/next day
SELECT sale_date, daily_revenue AS today,
    LAG(daily_revenue,1) OVER (ORDER BY sale_date) AS yesterday,
    LEAD(daily_revenue,1) OVER (ORDER BY sale_date) AS tomorrow,
    ROUND(daily_revenue - LAG(daily_revenue,1) OVER (ORDER BY sale_date),2) AS change_from_yesterday
FROM DAILY_SALES
WHERE sale_date BETWEEN '2024-06-01' AND '2024-06-10'
ORDER BY sale_date;

-- LAG with default value (avoid NULLs for the first row)
SELECT sale_date, daily_revenue,
    LAG(daily_revenue, 1, 0) OVER (ORDER BY sale_date) AS prev_or_zero,
    daily_revenue - LAG(daily_revenue, 1, 0) OVER (ORDER BY sale_date) AS diff
FROM DAILY_SALES
WHERE sale_date BETWEEN '2024-06-01' AND '2024-06-05'
ORDER BY sale_date;

-- ---------------------------------------------------------------------------
-- FIRST_VALUE / LAST_VALUE: grab a specific value from the window
-- ---------------------------------------------------------------------------

-- Best and worst day in the same month, shown on every row
SELECT sale_date, daily_revenue,
    DATE_TRUNC('MONTH', sale_date)::DATE AS month,
    FIRST_VALUE(daily_revenue) OVER (
        PARTITION BY DATE_TRUNC('MONTH', sale_date)
        ORDER BY daily_revenue DESC
    ) AS best_day_revenue,
    LAST_VALUE(daily_revenue) OVER (
        PARTITION BY DATE_TRUNC('MONTH', sale_date)
        ORDER BY daily_revenue DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS worst_day_revenue
FROM DAILY_SALES
WHERE sale_date BETWEEN '2024-06-01' AND '2024-06-15'
ORDER BY sale_date;

-- ---------------------------------------------------------------------------
-- SUM / AVG OVER: running totals and moving averages
-- ---------------------------------------------------------------------------

-- Running total (cumulative sum within each month)
SELECT sale_date, daily_revenue,
    SUM(daily_revenue) OVER (
        PARTITION BY DATE_TRUNC('MONTH', sale_date)
        ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_revenue
FROM DAILY_SALES
WHERE sale_date BETWEEN '2024-06-01' AND '2024-06-15';

-- 7-day rolling average
SELECT sale_date, daily_revenue,
    ROUND(AVG(daily_revenue) OVER (
        ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ),2) AS rolling_7day_avg
FROM DAILY_SALES
WHERE sale_date BETWEEN '2024-06-01' AND '2024-06-30';

-- Running MAX: highest revenue seen so far this month
SELECT sale_date, daily_revenue,
    MAX(daily_revenue) OVER (
        PARTITION BY DATE_TRUNC('MONTH', sale_date)
        ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_max
FROM DAILY_SALES
WHERE sale_date BETWEEN '2024-06-01' AND '2024-06-15';

-- ---------------------------------------------------------------------------
-- Window frame specs: ROWS vs RANGE
-- ROWS BETWEEN 2 PRECEDING AND CURRENT ROW  -> exact 3-row window
-- RANGE BETWEEN INTERVAL '7 DAYS' PRECEDING AND CURRENT ROW  -> date range
-- ---------------------------------------------------------------------------

-- 3-row moving average (ROWS-based)
SELECT sale_date, daily_revenue,
    ROUND(AVG(daily_revenue) OVER (
        ORDER BY sale_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) AS avg_3row
FROM DAILY_SALES
WHERE sale_date BETWEEN '2024-06-01' AND '2024-06-15'
ORDER BY sale_date;

-- ---------------------------------------------------------------------------
-- COUNT OVER: running count / total count per partition
-- ---------------------------------------------------------------------------

SELECT sale_date, daily_revenue,
    DATE_TRUNC('MONTH', sale_date)::DATE AS month,
    COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', sale_date)) AS days_in_month,
    COUNT(*) OVER (
        PARTITION BY DATE_TRUNC('MONTH', sale_date)
        ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_day_count
FROM DAILY_SALES
WHERE sale_date BETWEEN '2024-06-01' AND '2024-06-15'
ORDER BY sale_date;


-- ============================================================================
-- 1.5  Date Functions
-- ============================================================================
-- Docs: docs.snowflake.com/en/sql-reference/functions-date-time

-- ---------------------------------------------------------------------------
-- Core date functions at a glance
-- ---------------------------------------------------------------------------

SELECT
    CURRENT_DATE()                              AS today,
    CURRENT_TIMESTAMP()                         AS now_with_time,
    DATEADD(DAY, 30, CURRENT_DATE())            AS plus_30_days,
    DATEADD(MONTH, -3, CURRENT_DATE())          AS minus_3_months,
    DATEDIFF(DAY, '2024-01-01', '2024-12-31')   AS days_in_2024,
    DATE_TRUNC('MONTH', CURRENT_DATE())          AS start_of_month,
    DAYNAME(CURRENT_DATE())                      AS day_name,
    EXTRACT(QUARTER FROM CURRENT_DATE())         AS quarter,
    LAST_DAY(CURRENT_DATE(), 'MONTH')            AS end_of_month;

-- ---------------------------------------------------------------------------
-- DATEADD / DATEDIFF: date arithmetic
-- ---------------------------------------------------------------------------

-- Add and subtract various units
SELECT
    CURRENT_DATE()                           AS today,
    DATEADD(HOUR,   12, CURRENT_TIMESTAMP()) AS plus_12_hours,
    DATEADD(WEEK,   2,  CURRENT_DATE())      AS plus_2_weeks,
    DATEADD(YEAR,  -1,  CURRENT_DATE())      AS one_year_ago;

-- DATEDIFF with different units
SELECT
    DATEDIFF(DAY,    '2024-01-01', '2024-12-31') AS diff_days,
    DATEDIFF(WEEK,   '2024-01-01', '2024-12-31') AS diff_weeks,
    DATEDIFF(MONTH,  '2024-01-01', '2024-12-31') AS diff_months,
    DATEDIFF(YEAR,   '2024-01-01', '2025-06-15') AS diff_years;

-- TIMESTAMPDIFF: similar to DATEDIFF but for timestamps
SELECT
    TIMESTAMPDIFF(HOUR,   '2024-06-01 08:00', '2024-06-01 17:30') AS work_hours,
    TIMESTAMPDIFF(MINUTE, '2024-06-01 08:00', '2024-06-01 17:30') AS work_minutes;

-- ---------------------------------------------------------------------------
-- DATE_TRUNC: truncate to a given precision
-- ---------------------------------------------------------------------------

SELECT
    CURRENT_TIMESTAMP()                          AS now,
    DATE_TRUNC('SECOND',  CURRENT_TIMESTAMP())   AS trunc_second,
    DATE_TRUNC('MINUTE',  CURRENT_TIMESTAMP())   AS trunc_minute,
    DATE_TRUNC('HOUR',    CURRENT_TIMESTAMP())   AS trunc_hour,
    DATE_TRUNC('DAY',     CURRENT_TIMESTAMP())   AS trunc_day,
    DATE_TRUNC('WEEK',    CURRENT_DATE())         AS trunc_week,
    DATE_TRUNC('MONTH',   CURRENT_DATE())         AS trunc_month,
    DATE_TRUNC('QUARTER', CURRENT_DATE())         AS trunc_quarter,
    DATE_TRUNC('YEAR',    CURRENT_DATE())         AS trunc_year;

-- ---------------------------------------------------------------------------
-- EXTRACT / DATE_PART: pull out components
-- ---------------------------------------------------------------------------

SELECT
    CURRENT_TIMESTAMP()                          AS now,
    EXTRACT(YEAR    FROM CURRENT_DATE())          AS yr,
    EXTRACT(MONTH   FROM CURRENT_DATE())          AS mo,
    EXTRACT(DAY     FROM CURRENT_DATE())          AS dy,
    EXTRACT(HOUR    FROM CURRENT_TIMESTAMP())     AS hr,
    EXTRACT(DOW     FROM CURRENT_DATE())          AS day_of_week,     -- 0=Sun
    EXTRACT(DOY     FROM CURRENT_DATE())          AS day_of_year,
    EXTRACT(WEEK    FROM CURRENT_DATE())          AS week_number,
    EXTRACT(QUARTER FROM CURRENT_DATE())          AS quarter;

-- ---------------------------------------------------------------------------
-- Constructing dates from parts and converting formats
-- ---------------------------------------------------------------------------

SELECT
    DATE_FROM_PARTS(2024, 6, 15)                           AS built_date,
    TIMESTAMP_FROM_PARTS(2024, 6, 15, 14, 30, 0)          AS built_ts,
    TO_DATE('15/06/2024', 'DD/MM/YYYY')                    AS parsed_date,
    TO_TIMESTAMP('2024-06-15 14:30:00', 'YYYY-MM-DD HH24:MI:SS') AS parsed_ts,
    TO_VARCHAR(CURRENT_DATE(), 'Mon DD, YYYY')             AS formatted_1,
    TO_VARCHAR(CURRENT_DATE(), 'YYYY/MM/DD')               AS formatted_2,
    TO_VARCHAR(CURRENT_TIMESTAMP(), 'HH24:MI:SS')          AS time_only;

-- ---------------------------------------------------------------------------
-- Day/week/month helper functions
-- ---------------------------------------------------------------------------

SELECT
    DAYNAME(CURRENT_DATE())              AS day_name,       -- Mon, Tue, etc.
    DAYOFWEEK(CURRENT_DATE())            AS dow_num,        -- 0=Mon in ISO
    DAYOFMONTH(CURRENT_DATE())           AS dom,
    DAYOFYEAR(CURRENT_DATE())            AS doy,
    MONTHNAME(CURRENT_DATE())            AS month_name,     -- Jan, Feb, etc.
    WEEKOFYEAR(CURRENT_DATE())           AS week_num,
    LAST_DAY(CURRENT_DATE(), 'MONTH')    AS end_of_month,
    LAST_DAY(CURRENT_DATE(), 'WEEK')     AS end_of_week,
    LAST_DAY(CURRENT_DATE(), 'YEAR')     AS end_of_year;

-- ---------------------------------------------------------------------------
-- Time zones: CONVERT_TIMEZONE
-- ---------------------------------------------------------------------------

SELECT
    CURRENT_TIMESTAMP()                                             AS utc_now,
    CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', CURRENT_TIMESTAMP()) AS sao_paulo,
    CONVERT_TIMEZONE('UTC', 'America/New_York',  CURRENT_TIMESTAMP()) AS new_york,
    CONVERT_TIMEZONE('UTC', 'Asia/Tokyo',        CURRENT_TIMESTAMP()) AS tokyo;

-- ---------------------------------------------------------------------------
-- Practical example: age of each sale in days, weeks, months
-- ---------------------------------------------------------------------------

SELECT
    sale_date,
    DATEDIFF(DAY,   sale_date, CURRENT_DATE()) AS days_ago,
    DATEDIFF(WEEK,  sale_date, CURRENT_DATE()) AS weeks_ago,
    DATEDIFF(MONTH, sale_date, CURRENT_DATE()) AS months_ago,
    CASE
        WHEN DATEDIFF(DAY, sale_date, CURRENT_DATE()) <= 30  THEN 'RECENT'
        WHEN DATEDIFF(DAY, sale_date, CURRENT_DATE()) <= 90  THEN 'THIS QUARTER'
        WHEN DATEDIFF(DAY, sale_date, CURRENT_DATE()) <= 365 THEN 'THIS YEAR'
        ELSE 'OLDER'
    END AS age_bucket
FROM DAILY_SALES
ORDER BY sale_date DESC
LIMIT 10;


-- ============================================================================
-- 1.6  Semi-Structured Data (JSON, VARIANT, FLATTEN)
-- ============================================================================
-- Docs: docs.snowflake.com/en/sql-reference/data-types-semistructured

-- ---------------------------------------------------------------------------
-- Setup: table with VARIANT column for JSON data
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE CUSTOMER_EVENTS (
    event_id INT AUTOINCREMENT, event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    event_data VARIANT
);

INSERT INTO CUSTOMER_EVENTS (event_data) SELECT PARSE_JSON(column1) FROM VALUES
('{"user":"alice","action":"purchase","items":[{"name":"Laptop","price":999},{"name":"Mouse","price":29}]}'),
('{"user":"bob","action":"browse","pages":["home","products","cart"]}'),
('{"user":"carol","action":"purchase","items":[{"name":"Keyboard","price":75,"tags":["wireless","ergonomic"]}]}'),
('{"user":"dave","action":"signup","profile":{"age":30,"city":"Sao Paulo","interests":["data","sql"]}}');

-- ---------------------------------------------------------------------------
-- Accessing JSON fields: colon notation and casting
-- ---------------------------------------------------------------------------

-- Top-level fields with : and cast with ::
SELECT
    event_data:user::STRING   AS username,
    event_data:action::STRING AS action
FROM CUSTOMER_EVENTS;

-- Nested field access with : chaining
SELECT
    event_data:user::STRING            AS username,
    event_data:profile:city::STRING    AS city,
    event_data:profile:age::INT        AS age
FROM CUSTOMER_EVENTS
WHERE event_data:action = 'signup';

-- Array element access by index (0-based)
SELECT
    event_data:user::STRING           AS username,
    event_data:pages[0]::STRING       AS first_page,
    event_data:pages[2]::STRING       AS third_page,
    ARRAY_SIZE(event_data:pages)      AS num_pages
FROM CUSTOMER_EVENTS
WHERE event_data:action = 'browse';

-- ---------------------------------------------------------------------------
-- FLATTEN: explode arrays into rows
-- ---------------------------------------------------------------------------

-- Flatten items array from purchase events
SELECT
    e.event_data:user::STRING  AS username,
    f.value:name::STRING       AS item,
    f.value:price::FLOAT       AS price
FROM CUSTOMER_EVENTS e,
LATERAL FLATTEN(input => e.event_data:items) f
WHERE e.event_data:action = 'purchase';

-- FLATTEN with OUTER => TRUE: keep rows even if array is NULL/empty
-- (without OUTER, rows with no items would disappear)
SELECT
    e.event_data:user::STRING  AS username,
    e.event_data:action::STRING AS action,
    f.value:name::STRING       AS item
FROM CUSTOMER_EVENTS e,
LATERAL FLATTEN(input => e.event_data:items, OUTER => TRUE) f;

-- FLATTEN metadata columns: INDEX, KEY, PATH, THIS
SELECT
    e.event_data:user::STRING AS username,
    f.index                   AS array_position,
    f.value:name::STRING      AS item_name,
    f.path                    AS json_path
FROM CUSTOMER_EVENTS e,
LATERAL FLATTEN(input => e.event_data:items) f
WHERE e.event_data:action = 'purchase';

-- ---------------------------------------------------------------------------
-- Nested FLATTEN: arrays within arrays
-- ---------------------------------------------------------------------------

-- Flatten items, then flatten tags within each item
SELECT
    e.event_data:user::STRING  AS username,
    items.value:name::STRING   AS item_name,
    tags.value::STRING         AS tag
FROM CUSTOMER_EVENTS e,
LATERAL FLATTEN(input => e.event_data:items) items,
LATERAL FLATTEN(input => items.value:tags, OUTER => TRUE) tags
WHERE e.event_data:action = 'purchase';

-- ---------------------------------------------------------------------------
-- FLATTEN on OBJECT keys: turn key-value pairs into rows
-- ---------------------------------------------------------------------------

SELECT
    e.event_data:user::STRING AS username,
    f.key                     AS profile_field,
    f.value::STRING           AS profile_value
FROM CUSTOMER_EVENTS e,
LATERAL FLATTEN(input => e.event_data:profile) f
WHERE e.event_data:action = 'signup';

-- ---------------------------------------------------------------------------
-- OBJECT_CONSTRUCT: build JSON objects from columns
-- ---------------------------------------------------------------------------

SELECT OBJECT_CONSTRUCT(
    'date',    sale_date,
    'revenue', daily_revenue,
    'orders',  num_orders,
    'avg_order', ROUND(daily_revenue / NULLIF(num_orders, 0), 2)
) AS sale_json
FROM DAILY_SALES
WHERE sale_date BETWEEN '2024-06-01' AND '2024-06-03';

-- OBJECT_CONSTRUCT_KEEP_NULL: include NULL keys (default skips them)
SELECT OBJECT_CONSTRUCT_KEEP_NULL(
    'name',  'test',
    'value', NULL
) AS with_nulls;

-- ---------------------------------------------------------------------------
-- ARRAY functions: construct, aggregate, query
-- ---------------------------------------------------------------------------

-- ARRAY_CONSTRUCT: build an array literal
SELECT ARRAY_CONSTRUCT(1, 2, 3, 4, 5) AS my_array;

-- ARRAY_AGG: aggregate column values into an array per group
SELECT
    DATE_TRUNC('MONTH', sale_date)::DATE AS month,
    ARRAY_AGG(daily_revenue) WITHIN GROUP (ORDER BY sale_date) AS daily_revenues,
    ARRAY_SIZE(ARRAY_AGG(daily_revenue)) AS num_days
FROM DAILY_SALES
WHERE sale_date BETWEEN '2024-06-01' AND '2024-06-07'
GROUP BY month;

-- ARRAY_CONTAINS: check if a value exists in an array
SELECT
    event_data:user::STRING AS username,
    ARRAY_CONTAINS('home'::VARIANT, event_data:pages) AS visited_home
FROM CUSTOMER_EVENTS
WHERE event_data:action = 'browse';

-- ARRAY_SLICE: extract a sub-array
SELECT
    event_data:user::STRING AS username,
    event_data:pages AS all_pages,
    ARRAY_SLICE(event_data:pages, 0, 2) AS first_two_pages
FROM CUSTOMER_EVENTS
WHERE event_data:action = 'browse';

-- ---------------------------------------------------------------------------
-- PARSE_JSON / TO_JSON / TO_VARIANT: conversion functions
-- ---------------------------------------------------------------------------

SELECT
    PARSE_JSON('{"key": "value", "num": 42}') AS parsed,       -- STRING -> VARIANT
    TO_JSON(PARSE_JSON('{"key": "value"}'))    AS back_to_str,  -- VARIANT -> STRING
    TYPEOF(PARSE_JSON('{"key": "value"}'))     AS variant_type; -- shows OBJECT

-- TYPEOF: inspect the type of a VARIANT value
SELECT
    event_data:user,       TYPEOF(event_data:user)       AS user_type,
    event_data:items,      TYPEOF(event_data:items)      AS items_type,
    event_data:items[0]:price, TYPEOF(event_data:items[0]:price) AS price_type
FROM CUSTOMER_EVENTS
WHERE event_data:action = 'purchase'
LIMIT 1;

-- ---------------------------------------------------------------------------
-- Practical: aggregate JSON into a summary report
-- ---------------------------------------------------------------------------

SELECT
    e.event_data:user::STRING AS username,
    COUNT(*)                  AS num_items,
    SUM(f.value:price::FLOAT) AS total_spent,
    ARRAY_AGG(f.value:name::STRING) AS items_bought
FROM CUSTOMER_EVENTS e,
LATERAL FLATTEN(input => e.event_data:items) f
WHERE e.event_data:action = 'purchase'
GROUP BY username;


-- ############################################################################
--   LEVEL 2: SNOWFLAKE-SPECIFIC FEATURES
-- ############################################################################

-- ============================================================================
-- 2.1  Stages and COPY INTO (Data Loading)
-- ============================================================================
-- Docs: docs.snowflake.com/en/user-guide/data-load-overview
-- Guide: quickstarts.snowflake.com -> search data loading

-- ---------------------------------------------------------------------------
-- Internal Stage: created within Snowflake (no cloud credentials needed)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE STAGE MY_CSV_STAGE;

-- List files in the stage
LIST @MY_CSV_STAGE;

-- ---------------------------------------------------------------------------
-- File Formats: define how to parse files (CSV, JSON, Parquet, etc.)
-- ---------------------------------------------------------------------------

-- CSV file format with common options
CREATE OR REPLACE FILE FORMAT MY_CSV_FORMAT
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null')
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- JSON file format
CREATE OR REPLACE FILE FORMAT MY_JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE        -- if file is [{...},{...}]
    ALLOW_DUPLICATE = FALSE;

-- Parquet file format (columnar, compressed)
CREATE OR REPLACE FILE FORMAT MY_PARQUET_FORMAT
    TYPE = 'PARQUET'
    SNAPPY_COMPRESSION = TRUE;

-- ---------------------------------------------------------------------------
-- COPY INTO (Loading): stage -> table
-- ---------------------------------------------------------------------------

-- Basic CSV load (commented - requires actual files on stage)
-- PUT file://C:/data/sales.csv @MY_CSV_STAGE;
--
-- COPY INTO my_target_table
-- FROM @MY_CSV_STAGE/sales.csv
-- FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
-- ON_ERROR = 'CONTINUE';           -- skip bad rows

-- Load JSON into a VARIANT column
-- COPY INTO my_json_table (json_col)
-- FROM @MY_CSV_STAGE/events.json
-- FILE_FORMAT = (FORMAT_NAME = 'MY_JSON_FORMAT');

-- Load Parquet (auto column mapping by name)
-- COPY INTO my_parquet_table
-- FROM @MY_CSV_STAGE/data.parquet
-- FILE_FORMAT = (FORMAT_NAME = 'MY_PARQUET_FORMAT')
-- MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- ---------------------------------------------------------------------------
-- COPY options: controlling error handling and behavior
-- ---------------------------------------------------------------------------

-- ON_ERROR options:
--   'CONTINUE'          - skip bad rows, load the rest
--   'SKIP_FILE'         - skip entire file if any error
--   'SKIP_FILE_<N>'     - skip file if N+ errors
--   'ABORT_STATEMENT'   - stop immediately (default)

-- VALIDATION_MODE: dry run to preview errors without loading
-- COPY INTO my_table FROM @MY_CSV_STAGE/file.csv
--   FILE_FORMAT = (FORMAT_NAME = 'MY_CSV_FORMAT')
--   VALIDATION_MODE = 'RETURN_ERRORS';       -- show all errors
--   VALIDATION_MODE = 'RETURN_5_ROWS';       -- preview 5 rows

-- ---------------------------------------------------------------------------
-- COPY INTO (Unloading): table -> stage
-- ---------------------------------------------------------------------------

-- Unload query results to a stage as CSV
-- COPY INTO @MY_CSV_STAGE/export/sales_
-- FROM (SELECT sale_date, daily_revenue FROM DAILY_SALES WHERE sale_date >= '2024-06-01')
-- FILE_FORMAT = (TYPE='CSV' HEADER=TRUE)
-- OVERWRITE = TRUE
-- SINGLE = FALSE                    -- split into multiple files
-- MAX_FILE_SIZE = 50000000;         -- ~50MB per file

-- ---------------------------------------------------------------------------
-- Query files directly on stage (without loading)
-- ---------------------------------------------------------------------------

-- Preview CSV on stage before loading
-- SELECT $1, $2, $3 FROM @MY_CSV_STAGE/file.csv
-- (FILE_FORMAT => 'MY_CSV_FORMAT') LIMIT 10;

-- Query Parquet metadata
-- SELECT * FROM TABLE(INFER_SCHEMA(
--   LOCATION => '@MY_CSV_STAGE/data.parquet',
--   FILE_FORMAT => 'MY_PARQUET_FORMAT'
-- ));


-- ============================================================================
-- 2.2  Time Travel
-- ============================================================================
-- Docs: docs.snowflake.com/en/user-guide/data-time-travel
-- Query data as it was at a previous point in time. 1-90 day retention.

-- ---------------------------------------------------------------------------
-- Time Travel: query historical data (1-90 day retention)
-- ---------------------------------------------------------------------------

-- Create a demo table to show Time Travel in action
CREATE OR REPLACE TABLE TIME_TRAVEL_DEMO (id INT, name VARCHAR(30), value FLOAT);
INSERT INTO TIME_TRAVEL_DEMO VALUES (1,'alpha',100),(2,'beta',200),(3,'gamma',300);

-- Save the timestamp BEFORE making changes
SET before_change = CURRENT_TIMESTAMP();

-- Make a destructive change
UPDATE TIME_TRAVEL_DEMO SET value = value * 2 WHERE id = 1;
DELETE FROM TIME_TRAVEL_DEMO WHERE id = 3;

-- Current data (after changes)
SELECT * FROM TIME_TRAVEL_DEMO;

-- ---------------------------------------------------------------------------
-- AT (TIMESTAMP => ...): query data as it was at a specific time
-- ---------------------------------------------------------------------------

SELECT * FROM TIME_TRAVEL_DEMO AT (TIMESTAMP => $before_change);
-- Shows original data: alpha=100, gamma still exists

-- ---------------------------------------------------------------------------
-- AT (OFFSET => ...): query data N seconds ago
-- ---------------------------------------------------------------------------

-- Data as it was 60 seconds ago (if within retention)
-- SELECT * FROM TIME_TRAVEL_DEMO AT (OFFSET => -60);

-- ---------------------------------------------------------------------------
-- BEFORE (STATEMENT => ...): query data before a specific query ran
-- ---------------------------------------------------------------------------

-- Find the query ID of the UPDATE
-- SELECT QUERY_ID FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
-- WHERE QUERY_TEXT LIKE '%TIME_TRAVEL_DEMO%value * 2%' LIMIT 1;
-- Then:
-- SELECT * FROM TIME_TRAVEL_DEMO BEFORE (STATEMENT => '<query_id>');

-- ---------------------------------------------------------------------------
-- Restore data using Time Travel (INSERT from historical snapshot)
-- ---------------------------------------------------------------------------

-- Restore the deleted row
INSERT INTO TIME_TRAVEL_DEMO
SELECT * FROM TIME_TRAVEL_DEMO AT (TIMESTAMP => $before_change)
WHERE id = 3;

-- Verify: gamma is back
SELECT * FROM TIME_TRAVEL_DEMO ORDER BY id;

-- ---------------------------------------------------------------------------
-- UNDROP: recover dropped objects
-- ---------------------------------------------------------------------------

DROP TABLE TIME_TRAVEL_DEMO;
-- Oops! Bring it back:
UNDROP TABLE TIME_TRAVEL_DEMO;
SELECT * FROM TIME_TRAVEL_DEMO;  -- still there!

-- UNDROP works for schemas and databases too:
-- DROP SCHEMA my_schema;    ->  UNDROP SCHEMA my_schema;
-- DROP DATABASE my_db;      ->  UNDROP DATABASE my_db;

-- ---------------------------------------------------------------------------
-- DATA_RETENTION_TIME_IN_DAYS: control how far back Time Travel goes
-- ---------------------------------------------------------------------------

-- Check current retention
SHOW PARAMETERS LIKE 'DATA_RETENTION_TIME_IN_DAYS' IN TABLE TIME_TRAVEL_DEMO;

-- Set retention to 7 days (default is 1 for standard, up to 90 for enterprise)
-- ALTER TABLE TIME_TRAVEL_DEMO SET DATA_RETENTION_TIME_IN_DAYS = 7;


-- ============================================================================
-- 2.3  Zero-Copy Cloning
-- ============================================================================
-- Docs: docs.snowflake.com/en/user-guide/tables-storage-considerations
-- Instant copies, zero storage cost until data diverges.

-- ---------------------------------------------------------------------------
-- Clone a TABLE: instant, zero extra storage until data diverges
-- ---------------------------------------------------------------------------

-- Clone the demo table (uses the same underlying micro-partitions)
CREATE OR REPLACE TABLE TIME_TRAVEL_BACKUP CLONE TIME_TRAVEL_DEMO;

-- Both tables have the same data
SELECT 'ORIGINAL' AS source, * FROM TIME_TRAVEL_DEMO
UNION ALL
SELECT 'CLONE' AS source, * FROM TIME_TRAVEL_BACKUP
ORDER BY source, id;

-- Changes to one table do NOT affect the other
INSERT INTO TIME_TRAVEL_BACKUP VALUES (99, 'clone_only', 999);
SELECT * FROM TIME_TRAVEL_BACKUP ORDER BY id;  -- has row 99
SELECT * FROM TIME_TRAVEL_DEMO ORDER BY id;    -- does NOT have row 99

-- ---------------------------------------------------------------------------
-- Clone at a specific point in time (Time Travel + Clone)
-- ---------------------------------------------------------------------------

-- Clone the table as it was before our earlier changes
-- CREATE TABLE time_travel_snapshot CLONE TIME_TRAVEL_DEMO
--   AT (TIMESTAMP => $before_change);

-- ---------------------------------------------------------------------------
-- Clone at higher levels: SCHEMA and DATABASE
-- ---------------------------------------------------------------------------

-- Clone an entire schema (all tables, views, stages, etc.)
-- CREATE SCHEMA dev_practice CLONE PRACTICE;

-- Clone an entire database (all schemas + objects)
-- CREATE DATABASE staging_db CLONE SALES_FORECAST_LAB;

-- ---------------------------------------------------------------------------
-- Check clone metadata
-- ---------------------------------------------------------------------------

-- See if a table is a clone and when it was cloned
-- SELECT * FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
-- WHERE TABLE_NAME = 'TIME_TRAVEL_BACKUP';


-- ============================================================================
-- 2.4  Tasks and Streams (Scheduling + CDC)
-- ============================================================================
-- Docs: docs.snowflake.com/en/user-guide/streams
-- Docs: docs.snowflake.com/en/user-guide/tasks-intro

-- ---------------------------------------------------------------------------
-- STREAMS: Change Data Capture (CDC) - track INSERT, UPDATE, DELETE
-- ---------------------------------------------------------------------------

-- Standard stream on DAILY_SALES (tracks all DML changes)
CREATE OR REPLACE STREAM SALES_CHANGES ON TABLE DAILY_SALES;

-- Check the stream (empty until changes happen after stream creation)
SELECT * FROM SALES_CHANGES;

-- Streams add metadata columns:
--   METADATA$ACTION   = 'INSERT' or 'DELETE'
--   METADATA$ISUPDATE = TRUE if the row is part of an UPDATE
--   METADATA$ROW_ID   = unique row identifier

-- ---------------------------------------------------------------------------
-- Demo: make changes and observe the stream
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE STREAM_DEMO (id INT, val VARCHAR(30));
CREATE OR REPLACE STREAM STREAM_DEMO_CDC ON TABLE STREAM_DEMO;

-- Insert rows
INSERT INTO STREAM_DEMO VALUES (1,'original'),(2,'original');

-- See what the stream captured
SELECT * FROM STREAM_DEMO_CDC;
-- Shows 2 INSERT rows

-- Update a row (appears as DELETE old + INSERT new)
UPDATE STREAM_DEMO SET val = 'updated' WHERE id = 1;
SELECT * FROM STREAM_DEMO_CDC;

-- Delete a row
DELETE FROM STREAM_DEMO WHERE id = 2;
SELECT * FROM STREAM_DEMO_CDC;

-- ---------------------------------------------------------------------------
-- Stream types: STANDARD vs APPEND_ONLY
-- ---------------------------------------------------------------------------

-- APPEND_ONLY: only tracks INSERTs (more efficient for insert-heavy tables)
CREATE OR REPLACE STREAM SALES_APPEND_ONLY
    ON TABLE DAILY_SALES APPEND_ONLY = TRUE;

-- ---------------------------------------------------------------------------
-- Consuming a stream: reading it resets it (within a DML transaction)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE STREAM_DEMO_LOG (
    id INT, val VARCHAR(30), change_type VARCHAR(10), captured_at TIMESTAMP
);

-- This INSERT consumes the stream (resets it to empty after commit)
INSERT INTO STREAM_DEMO_LOG
SELECT id, val,
    CASE WHEN METADATA$ACTION = 'INSERT' AND NOT METADATA$ISUPDATE THEN 'INSERT'
         WHEN METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE     THEN 'UPDATE'
         WHEN METADATA$ACTION = 'DELETE' AND NOT METADATA$ISUPDATE  THEN 'DELETE'
    END AS change_type,
    CURRENT_TIMESTAMP()
FROM STREAM_DEMO_CDC;

-- Stream is now empty
SELECT * FROM STREAM_DEMO_CDC;  -- 0 rows

-- Log has the history
SELECT * FROM STREAM_DEMO_LOG;

-- ---------------------------------------------------------------------------
-- SYSTEM$STREAM_HAS_DATA: check if stream has new data (for task WHEN)
-- ---------------------------------------------------------------------------

SELECT SYSTEM$STREAM_HAS_DATA('STREAM_DEMO_CDC') AS has_data;

-- ---------------------------------------------------------------------------
-- TASKS: schedule SQL to run automatically
-- ---------------------------------------------------------------------------

-- Simple scheduled task (runs every 5 minutes)
-- CREATE OR REPLACE TASK SALES_SUMMARY_TASK
--     WAREHOUSE = LATAM_LAB_WH
--     SCHEDULE = '5 MINUTE'
-- AS
--     INSERT INTO sales_summary
--     SELECT DATE_TRUNC('HOUR', CURRENT_TIMESTAMP()) AS hour,
--            COUNT(*) AS new_rows
--     FROM SALES_CHANGES;

-- Cron-based schedule (every hour at minute 0, Sao Paulo timezone)
-- CREATE OR REPLACE TASK HOURLY_TASK
--     WAREHOUSE = LATAM_LAB_WH
--     SCHEDULE = 'USING CRON 0 * * * * America/Sao_Paulo'
-- AS
--     CALL my_procedure();

-- ---------------------------------------------------------------------------
-- Conditional task: only run when the stream has data
-- ---------------------------------------------------------------------------

-- CREATE OR REPLACE TASK PROCESS_SALES_CHANGES
--     WAREHOUSE = LATAM_LAB_WH
--     SCHEDULE = '5 MINUTE'
--     WHEN SYSTEM$STREAM_HAS_DATA('SALES_CHANGES')
-- AS
--     INSERT INTO sales_change_log
--     SELECT *, CURRENT_TIMESTAMP() AS processed_at
--     FROM SALES_CHANGES;

-- ---------------------------------------------------------------------------
-- Task trees: parent -> child dependencies (DAG)
-- ---------------------------------------------------------------------------

-- Root task (has SCHEDULE)
-- CREATE OR REPLACE TASK ROOT_TASK
--     WAREHOUSE = LATAM_LAB_WH
--     SCHEDULE = '30 MINUTE'
-- AS SELECT 1;

-- Child task (runs AFTER parent completes, no SCHEDULE)
-- CREATE OR REPLACE TASK CHILD_TASK_A
--     WAREHOUSE = LATAM_LAB_WH
--     AFTER ROOT_TASK
-- AS SELECT 2;

-- Another child (can depend on multiple parents)
-- CREATE OR REPLACE TASK CHILD_TASK_B
--     WAREHOUSE = LATAM_LAB_WH
--     AFTER ROOT_TASK
-- AS SELECT 3;

-- ---------------------------------------------------------------------------
-- Task management: resume, suspend, monitor
-- ---------------------------------------------------------------------------

-- Tasks are created SUSPENDED by default. Must resume to activate:
-- ALTER TASK ROOT_TASK RESUME;
-- ALTER TASK CHILD_TASK_A RESUME;
-- ALTER TASK CHILD_TASK_B RESUME;

-- Suspend a task:
-- ALTER TASK ROOT_TASK SUSPEND;

-- Check task history:
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
-- ORDER BY SCHEDULED_TIME DESC LIMIT 20;

-- Show all tasks:
-- SHOW TASKS;


-- ############################################################################
--   LEVEL 3: ADVANCED / HIGH-VALUE FEATURES
-- ############################################################################

-- ============================================================================
-- 3.1  Dynamic Tables (Declarative Pipelines)
-- ============================================================================
-- Docs: docs.snowflake.com/en/user-guide/dynamic-tables-about
-- Auto-refreshing materialized views. Replace Task+Stream pipelines.

-- ---------------------------------------------------------------------------
-- What are Dynamic Tables?
-- Declarative SQL pipelines that auto-refresh. You define the WHAT (the query),
-- Snowflake handles the WHEN (refresh scheduling based on TARGET_LAG).
-- They replace manual Task + Stream pipelines for many use cases.
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- Basic Dynamic Table: monthly revenue summary
-- ---------------------------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE MONTHLY_REVENUE_SUMMARY
    TARGET_LAG = '1 hour' WAREHOUSE = LATAM_LAB_WH
AS SELECT
    DATE_TRUNC('MONTH', sale_date)::DATE AS month,
    SUM(daily_revenue) AS total_revenue, SUM(num_orders) AS total_orders,
    AVG(daily_revenue) AS avg_daily, MAX(daily_revenue) AS best_day
FROM DAILY_SALES GROUP BY month;

-- Query it like a regular table
SELECT * FROM MONTHLY_REVENUE_SUMMARY ORDER BY month;

-- ---------------------------------------------------------------------------
-- Chaining Dynamic Tables: downstream DT reads from upstream DT
-- Snowflake automatically manages the refresh order
-- ---------------------------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE QUARTERLY_REVENUE
    TARGET_LAG = '1 hour' WAREHOUSE = LATAM_LAB_WH
AS SELECT DATE_TRUNC('QUARTER', month)::DATE AS quarter,
    SUM(total_revenue) AS quarterly_revenue,
    SUM(total_orders)  AS quarterly_orders,
    ROUND(SUM(total_revenue) / NULLIF(SUM(total_orders), 0), 2) AS revenue_per_order
FROM MONTHLY_REVENUE_SUMMARY GROUP BY quarter;

-- Another downstream: year-over-year comparison
CREATE OR REPLACE DYNAMIC TABLE YEARLY_REVENUE
    TARGET_LAG = '1 hour' WAREHOUSE = LATAM_LAB_WH
AS SELECT
    EXTRACT(YEAR FROM quarter)    AS year,
    SUM(quarterly_revenue)        AS annual_revenue,
    SUM(quarterly_orders)         AS annual_orders
FROM QUARTERLY_REVENUE
GROUP BY year;

-- ---------------------------------------------------------------------------
-- TARGET_LAG options
-- ---------------------------------------------------------------------------

-- Time-based lag (refresh at most this often):
--   TARGET_LAG = '1 minute'    -- near real-time
--   TARGET_LAG = '5 minutes'
--   TARGET_LAG = '1 hour'      -- common for dashboards
--   TARGET_LAG = '1 day'       -- daily batch

-- DOWNSTREAM: refresh only when a downstream DT needs fresh data
--   TARGET_LAG = DOWNSTREAM

-- ---------------------------------------------------------------------------
-- Monitoring Dynamic Tables
-- ---------------------------------------------------------------------------

-- Check refresh history
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    NAME => 'MONTHLY_REVENUE_SUMMARY'
)) ORDER BY REFRESH_START_TIME DESC LIMIT 10;

-- Check all dynamic tables and their lag
SHOW DYNAMIC TABLES;

-- Graph history: see the DAG of refreshes
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_GRAPH_HISTORY())
ORDER BY REFRESH_START_TIME DESC LIMIT 20;

-- ---------------------------------------------------------------------------
-- Alter / Suspend / Resume a Dynamic Table
-- ---------------------------------------------------------------------------

-- Change the lag
-- ALTER DYNAMIC TABLE MONTHLY_REVENUE_SUMMARY SET TARGET_LAG = '30 minutes';

-- Suspend refreshes (table becomes static)
-- ALTER DYNAMIC TABLE MONTHLY_REVENUE_SUMMARY SUSPEND;

-- Resume refreshes
-- ALTER DYNAMIC TABLE MONTHLY_REVENUE_SUMMARY RESUME;

-- Force an immediate manual refresh
-- ALTER DYNAMIC TABLE MONTHLY_REVENUE_SUMMARY REFRESH;


-- ============================================================================
-- 3.2  Snowflake ML (Built-in Machine Learning)
-- ============================================================================
-- Docs: docs.snowflake.com/en/user-guide/ml-powered-overview
-- FORECAST, ANOMALY_DETECTION, CLASSIFICATION, TOP_INSIGHTS
-- See the forecast project file for a full working example!

-- ---------------------------------------------------------------------------
-- FORECAST: time-series prediction built into SQL
-- ---------------------------------------------------------------------------

-- Prepare training data: daily revenue as a time series
CREATE OR REPLACE VIEW FORECAST_INPUT AS
SELECT sale_date AS ds, daily_revenue AS y
FROM DAILY_SALES
WHERE sale_date >= '2024-01-01'
ORDER BY ds;

-- Build a forecast model (trains automatically)
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST REVENUE_FORECAST_MODEL(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'FORECAST_INPUT'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'Y'
);

-- Generate predictions for the next 30 days
CALL REVENUE_FORECAST_MODEL!FORECAST(FORECASTING_PERIODS => 30);

-- Forecast with prediction intervals (default 95% confidence)
CALL REVENUE_FORECAST_MODEL!FORECAST(
    FORECASTING_PERIODS => 30,
    CONFIG_OBJECT => {'prediction_interval': 0.90}
);

-- ---------------------------------------------------------------------------
-- ANOMALY_DETECTION: find unusual data points
-- ---------------------------------------------------------------------------

-- Build an anomaly detection model on the same time series
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION REVENUE_ANOMALY_MODEL(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'FORECAST_INPUT'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'Y'
);

-- Detect anomalies in new data
CALL REVENUE_ANOMALY_MODEL!DETECT_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'FORECAST_INPUT'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'Y',
    CONFIG_OBJECT => {'prediction_interval': 0.99}
);
-- Returns IS_ANOMALY = TRUE for outlier days

-- ---------------------------------------------------------------------------
-- CLASSIFICATION: predict categories from features
-- ---------------------------------------------------------------------------

-- Setup: create labeled training data
CREATE OR REPLACE TEMP TABLE CLASSIFICATION_DATA AS
SELECT
    daily_revenue,
    num_orders,
    DAYOFWEEK(sale_date) AS day_of_week,
    CASE
        WHEN daily_revenue >= 200 THEN 'HIGH'
        WHEN daily_revenue >= 120 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS revenue_class
FROM DAILY_SALES
WHERE sale_date >= '2024-01-01';

-- Build a classification model
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION REVENUE_CLASSIFIER(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'CLASSIFICATION_DATA'),
    TARGET_COLNAME => 'REVENUE_CLASS'
);

-- Predict on new data
CREATE OR REPLACE TEMP TABLE NEW_DATA AS
SELECT 180.0 AS daily_revenue, 12 AS num_orders, 3 AS day_of_week
UNION ALL
SELECT 95.0, 5, 1
UNION ALL
SELECT 280.0, 20, 5;

SELECT *, REVENUE_CLASSIFIER!PREDICT(
    INPUT_DATA => OBJECT_CONSTRUCT(*)
) AS prediction
FROM NEW_DATA;

-- ---------------------------------------------------------------------------
-- TOP_INSIGHTS: automated root-cause analysis
-- ---------------------------------------------------------------------------

-- "Why did revenue change between these two periods?"
-- CREATE OR REPLACE SNOWFLAKE.ML.TOP_INSIGHTS REVENUE_INSIGHTS(
--     INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'analysis_table'),
--     TARGET_COLNAME => 'daily_revenue',
--     CATEGORICAL_DIMENSIONS => ['category_name', 'region'],
--     METRIC => 'SUM',
--     LABEL_COLNAME => 'period',      -- 'before' vs 'after'
--     LABEL_VALUES => ARRAY_CONSTRUCT('before', 'after')
-- );
-- CALL REVENUE_INSIGHTS!GET_INSIGHTS();


-- ============================================================================
-- 3.3  Cortex AI Functions (LLM in SQL)
-- ============================================================================
-- Docs: docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions

-- ---------------------------------------------------------------------------
-- Setup: sample review data
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE CUSTOMER_REVIEWS (
    review_id INT, product VARCHAR(50), review_text VARCHAR(1000)
);
INSERT INTO CUSTOMER_REVIEWS VALUES
(1,'Laptop','Amazing laptop! Super fast. Best purchase this year.'),
(2,'Laptop','Terrible. Keyboard broke after 2 weeks. Returning it.'),
(3,'Mouse','Decent for the price. Gets the job done.'),
(4,'Monitor','Beautiful 4K display. Colors are vivid. A bit expensive though.'),
(5,'Keyboard','Mechanical keys feel great but it is too loud for the office.');

-- ---------------------------------------------------------------------------
-- SENTIMENT: score text from -1 (negative) to +1 (positive)
-- ---------------------------------------------------------------------------

SELECT review_id, product, review_text,
    ROUND(SNOWFLAKE.CORTEX.SENTIMENT(review_text), 3) AS sentiment
FROM CUSTOMER_REVIEWS
ORDER BY sentiment;

-- Aggregate: average sentiment per product
SELECT product,
    COUNT(*) AS num_reviews,
    ROUND(AVG(SNOWFLAKE.CORTEX.SENTIMENT(review_text)), 3) AS avg_sentiment
FROM CUSTOMER_REVIEWS
GROUP BY product
ORDER BY avg_sentiment DESC;

-- ---------------------------------------------------------------------------
-- SUMMARIZE: condense long text into a brief summary
-- ---------------------------------------------------------------------------

SELECT review_id,
    SNOWFLAKE.CORTEX.SUMMARIZE(review_text) AS summary
FROM CUSTOMER_REVIEWS
WHERE review_id = 1;

-- Summarize all reviews for a product into one summary
SELECT SNOWFLAKE.CORTEX.SUMMARIZE(
    LISTAGG(review_text, '. ') WITHIN GROUP (ORDER BY review_id)
) AS laptop_summary
FROM CUSTOMER_REVIEWS
WHERE product = 'Laptop';

-- ---------------------------------------------------------------------------
-- TRANSLATE: translate text between languages
-- ---------------------------------------------------------------------------

SELECT review_id,
    SNOWFLAKE.CORTEX.TRANSLATE(review_text, 'en', 'pt') AS portugues,
    SNOWFLAKE.CORTEX.TRANSLATE(review_text, 'en', 'es') AS espanol
FROM CUSTOMER_REVIEWS
WHERE review_id = 1;

-- ---------------------------------------------------------------------------
-- EXTRACT_ANSWER: question answering from a context document
-- ---------------------------------------------------------------------------

SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
    'Laptop: 16GB RAM, 512GB SSD, 14in display, 1.4kg, 12h battery.',
    'How much does it weigh?'
) AS answer;

SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
    'Laptop: 16GB RAM, 512GB SSD, 14in display, 1.4kg, 12h battery.',
    'What is the storage capacity?'
) AS answer;

-- Extract answer from review text
SELECT review_id,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(review_text, 'What went wrong?') AS issue
FROM CUSTOMER_REVIEWS
WHERE SNOWFLAKE.CORTEX.SENTIMENT(review_text) < 0;

-- ---------------------------------------------------------------------------
-- COMPLETE: general-purpose LLM text generation
-- ---------------------------------------------------------------------------

-- Simple prompt
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'snowflake-arctic',
    'Write a one-sentence product tagline for a fast laptop.'
) AS tagline;

-- Structured prompt with system + user messages
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'snowflake-arctic',
    [
        {'role': 'system', 'content': 'You are a helpful product analyst. Be concise.'},
        {'role': 'user',   'content': 'Categorize this review as BUG, FEATURE_REQUEST, or PRAISE: "Keyboard broke after 2 weeks"'}
    ]
) AS category;

-- Use COMPLETE to classify each review
SELECT review_id, product, review_text,
    SNOWFLAKE.CORTEX.COMPLETE(
        'snowflake-arctic',
        'Classify this review as POSITIVE, NEGATIVE, or MIXED. Reply with one word only: ' || review_text
    ) AS llm_classification
FROM CUSTOMER_REVIEWS;

-- ---------------------------------------------------------------------------
-- CLASSIFY_TEXT: structured text classification
-- ---------------------------------------------------------------------------

SELECT review_id,
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        review_text,
        ['Positive', 'Negative', 'Mixed']
    ) AS classification
FROM CUSTOMER_REVIEWS;

-- ---------------------------------------------------------------------------
-- Combining Cortex functions: sentiment + classification pipeline
-- ---------------------------------------------------------------------------

SELECT
    review_id,
    product,
    LEFT(review_text, 50) || '...' AS preview,
    ROUND(SNOWFLAKE.CORTEX.SENTIMENT(review_text), 3) AS sentiment_score,
    CASE
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(review_text) > 0.3  THEN 'POSITIVE'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(review_text) < -0.3 THEN 'NEGATIVE'
        ELSE 'NEUTRAL'
    END AS sentiment_label
FROM CUSTOMER_REVIEWS
ORDER BY sentiment_score;


-- ============================================================================
-- 3.4  Snowpark Python UDFs
-- ============================================================================
-- Docs: docs.snowflake.com/en/developer-guide/snowpark/python/index

-- ---------------------------------------------------------------------------
-- Scalar UDF: one input row -> one output value
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION CLASSIFY_REVENUE(revenue FLOAT)
RETURNS VARCHAR LANGUAGE PYTHON RUNTIME_VERSION='3.11' HANDLER='classify'
AS '
def classify(revenue):
    if revenue >= 250: return "HIGH"
    elif revenue >= 150: return "MEDIUM"
    elif revenue >= 100: return "LOW"
    else: return "VERY LOW"
';

SELECT sale_date, daily_revenue, CLASSIFY_REVENUE(daily_revenue) AS class
FROM DAILY_SALES WHERE sale_date >= '2024-12-01' ORDER BY sale_date;

-- ---------------------------------------------------------------------------
-- Scalar UDF with multiple parameters
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION CALC_MARGIN(revenue FLOAT, cost_pct FLOAT)
RETURNS FLOAT LANGUAGE PYTHON RUNTIME_VERSION='3.11' HANDLER='calc'
AS '
def calc(revenue, cost_pct):
    if revenue is None or cost_pct is None:
        return None
    return round(revenue * (1 - cost_pct), 2)
';

SELECT sale_date, daily_revenue,
    CALC_MARGIN(daily_revenue, 0.35) AS margin_65pct
FROM DAILY_SALES
WHERE sale_date BETWEEN '2024-06-01' AND '2024-06-05';

-- ---------------------------------------------------------------------------
-- Vectorized UDF (VECTORIZED): process batches with pandas for performance
-- Much faster than row-by-row for large datasets
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION ZSCORE(val FLOAT)
RETURNS FLOAT
LANGUAGE PYTHON RUNTIME_VERSION='3.11'
PACKAGES = ('pandas')
HANDLER = 'zscore_handler'
AS '
import pandas as pd

class zscore_handler:
    def __init__(self):
        self._all_values = []

    def process(self, val):
        # row-by-row fallback: just return the value
        return float(val) if val is not None else None

    def end_partition(self):
        pass
';

-- Simpler: inline vectorized with @udf decorator pattern
-- (Using a regular scalar UDF that leverages numpy via PACKAGES)
CREATE OR REPLACE FUNCTION NORMALIZE_REVENUE(revenue FLOAT, min_val FLOAT, max_val FLOAT)
RETURNS FLOAT LANGUAGE PYTHON RUNTIME_VERSION='3.11' HANDLER='normalize'
AS '
def normalize(revenue, min_val, max_val):
    if revenue is None or min_val is None or max_val is None:
        return None
    if max_val == min_val:
        return 0.0
    return round((revenue - min_val) / (max_val - min_val), 4)
';

-- Use with a subquery to get min/max
SELECT sale_date, daily_revenue,
    NORMALIZE_REVENUE(daily_revenue, stats.min_rev, stats.max_rev) AS normalized
FROM DAILY_SALES
CROSS JOIN (
    SELECT MIN(daily_revenue) AS min_rev, MAX(daily_revenue) AS max_rev
    FROM DAILY_SALES
) stats
WHERE sale_date BETWEEN '2024-06-01' AND '2024-06-10'
ORDER BY sale_date;

-- ---------------------------------------------------------------------------
-- UDF with external packages (PACKAGES clause)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION PARSE_EMAIL_DOMAIN(email VARCHAR)
RETURNS VARCHAR LANGUAGE PYTHON RUNTIME_VERSION='3.11' HANDLER='get_domain'
AS '
def get_domain(email):
    if email is None or "@" not in email:
        return None
    return email.split("@")[1].lower()
';

SELECT PARSE_EMAIL_DOMAIN('user@snowflake.com') AS domain;

-- ---------------------------------------------------------------------------
-- UDTF (User-Defined Table Function): one input -> multiple output rows
-- ---------------------------------------------------------------------------

-- Split a comma-separated string into rows
CREATE OR REPLACE FUNCTION SPLIT_TO_ROWS(input_str VARCHAR, delimiter VARCHAR)
RETURNS TABLE (item VARCHAR, position INT)
LANGUAGE PYTHON RUNTIME_VERSION='3.11' HANDLER='SplitHandler'
AS '
class SplitHandler:
    def process(self, input_str, delimiter):
        if input_str is None:
            return
        parts = input_str.split(delimiter if delimiter else ",")
        for i, part in enumerate(parts):
            yield (part.strip(), i)
';

-- Use with TABLE() and LATERAL
SELECT t.item, t.position
FROM TABLE(SPLIT_TO_ROWS('apple, banana, cherry, date', ',')) t;

-- UDTF with JOIN: split tags column for each product
CREATE OR REPLACE TEMP TABLE PRODUCTS_WITH_TAGS (
    product_id INT, product_name VARCHAR(30), tags VARCHAR(100)
);
INSERT INTO PRODUCTS_WITH_TAGS VALUES
    (1, 'Laptop',   'electronics, portable, work'),
    (2, 'Mouse',    'electronics, peripheral'),
    (3, 'Notebook', 'stationery, office');

SELECT p.product_name, t.item AS tag, t.position
FROM PRODUCTS_WITH_TAGS p,
LATERAL TABLE(SPLIT_TO_ROWS(p.tags, ',')) t
ORDER BY p.product_name, t.position;

-- ---------------------------------------------------------------------------
-- SQL UDF: simpler alternative when Python is not needed
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION REVENUE_TIER(revenue FLOAT)
RETURNS VARCHAR
AS $$
    CASE
        WHEN revenue >= 250 THEN 'HIGH'
        WHEN revenue >= 150 THEN 'MEDIUM'
        WHEN revenue >= 100 THEN 'LOW'
        ELSE 'VERY LOW'
    END
$$;

SELECT sale_date, daily_revenue, REVENUE_TIER(daily_revenue) AS tier
FROM DAILY_SALES
WHERE sale_date >= '2024-12-01'
ORDER BY sale_date;


-- ============================================================================
-- 3.5  Streamlit in Snowflake
-- ============================================================================
-- Docs: docs.snowflake.com/en/developer-guide/streamlit/about-streamlit
-- Create in Snowsight: Projects > Streamlit > + Streamlit App
--
-- HOW IT WORKS:
--   1. Go to Snowsight > Projects > Streamlit > + Streamlit App
--   2. Choose warehouse and database/schema
--   3. Paste one of the Python examples below
--   4. Click "Run" - the app renders in your browser, running inside Snowflake
--
-- Key concepts:
--   - get_active_session() gives you a Snowpark session (no credentials needed)
--   - All data stays inside Snowflake (no data leaves the platform)
--   - You can use st.* widgets for interactivity (filters, buttons, etc.)

-- ---------------------------------------------------------------------------
-- Example 1: Simple Sales Dashboard (starter)
-- ---------------------------------------------------------------------------
-- import streamlit as st
-- from snowflake.snowpark.context import get_active_session
--
-- session = get_active_session()
-- st.title("Sales Dashboard")
--
-- # Load data
-- df = session.sql("""
--     SELECT sale_date, daily_revenue, num_orders
--     FROM SALES_FORECAST_LAB.PRACTICE.DAILY_SALES
--     WHERE sale_date >= '2024-01-01'
--     ORDER BY sale_date
-- """).to_pandas()
--
-- # KPI metrics
-- c1, c2, c3 = st.columns(3)
-- c1.metric("Total Revenue", f"${df['DAILY_REVENUE'].sum():,.0f}")
-- c2.metric("Avg Daily Revenue", f"${df['DAILY_REVENUE'].mean():,.2f}")
-- c3.metric("Total Orders", f"{df['NUM_ORDERS'].sum():,}")
--
-- # Line chart
-- st.subheader("Daily Revenue Over Time")
-- st.line_chart(df.set_index('SALE_DATE')['DAILY_REVENUE'])

-- ---------------------------------------------------------------------------
-- Example 2: Interactive Dashboard with Filters
-- ---------------------------------------------------------------------------
-- import streamlit as st
-- from snowflake.snowpark.context import get_active_session
-- import pandas as pd
--
-- session = get_active_session()
-- st.title("Interactive Sales Explorer")
--
-- # Sidebar filters
-- st.sidebar.header("Filters")
-- min_rev = st.sidebar.slider("Min Daily Revenue", 0, 500, 100)
-- date_range = st.sidebar.date_input("Date Range",
--     value=(pd.Timestamp("2024-01-01"), pd.Timestamp("2024-12-31")))
--
-- # Query with filters
-- df = session.sql(f"""
--     SELECT sale_date, daily_revenue, num_orders,
--            daily_revenue / NULLIF(num_orders, 0) AS rev_per_order
--     FROM SALES_FORECAST_LAB.PRACTICE.DAILY_SALES
--     WHERE daily_revenue >= {min_rev}
--       AND sale_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'
--     ORDER BY sale_date
-- """).to_pandas()
--
-- st.write(f"Showing {len(df)} days matching filters")
--
-- # Tabs for different views
-- tab1, tab2 = st.tabs(["Chart", "Data Table"])
-- with tab1:
--     st.line_chart(df.set_index('SALE_DATE')[['DAILY_REVENUE', 'NUM_ORDERS']])
-- with tab2:
--     st.dataframe(df, use_container_width=True)

-- ---------------------------------------------------------------------------
-- Example 3: Monthly Summary with Bar Chart
-- ---------------------------------------------------------------------------
-- import streamlit as st
-- from snowflake.snowpark.context import get_active_session
--
-- session = get_active_session()
-- st.title("Monthly Revenue Summary")
--
-- df = session.sql("""
--     SELECT DATE_TRUNC('MONTH', sale_date)::DATE AS month,
--            SUM(daily_revenue) AS total_revenue,
--            SUM(num_orders) AS total_orders,
--            COUNT(*) AS selling_days
--     FROM SALES_FORECAST_LAB.PRACTICE.DAILY_SALES
--     WHERE sale_date >= '2024-01-01'
--     GROUP BY month ORDER BY month
-- """).to_pandas()
--
-- st.bar_chart(df.set_index('MONTH')['TOTAL_REVENUE'])
--
-- # Show best/worst month
-- best = df.loc[df['TOTAL_REVENUE'].idxmax()]
-- worst = df.loc[df['TOTAL_REVENUE'].idxmin()]
-- c1, c2 = st.columns(2)
-- c1.metric("Best Month", f"{best['MONTH']}", f"${best['TOTAL_REVENUE']:,.0f}")
-- c2.metric("Worst Month", f"{worst['MONTH']}", f"${worst['TOTAL_REVENUE']:,.0f}")
--
-- st.dataframe(df, use_container_width=True)


-- ============================================================================
-- LEARNING PATH (all free)
-- ============================================================================
-- 1. trial.snowflake.com            -> 30-day free trial
-- 2. quickstarts.snowflake.com      -> Guided hands-on tutorials
-- 3. learn.snowflake.com            -> Courses and certifications
-- 4. docs.snowflake.com             -> Reference documentation
-- 5. youtube.com/@SnowflakeInc      -> BUILD series, Summit talks
-- 6. community.snowflake.com        -> Q and A forum
-- 7. medium.com/snowflake           -> Technical blog
-- ============================================================================

-- CLEANUP (uncomment to delete all objects created by this guide)
-- DROP TABLE IF EXISTS PRODUCT_CATEGORIES;
-- DROP TABLE IF EXISTS SALES_BY_CATEGORY;
-- DROP TABLE IF EXISTS MYSTERY_SALES;
-- DROP TABLE IF EXISTS REGIONS;
-- DROP TABLE IF EXISTS CATEGORY_REGIONS;
-- DROP TABLE IF EXISTS CUSTOMER_EVENTS;
-- DROP TABLE IF EXISTS CUSTOMER_REVIEWS;
-- DROP TABLE IF EXISTS TIME_TRAVEL_DEMO;
-- DROP TABLE IF EXISTS TIME_TRAVEL_BACKUP;
-- DROP TABLE IF EXISTS STREAM_DEMO;
-- DROP TABLE IF EXISTS STREAM_DEMO_LOG;
-- DROP VIEW IF EXISTS FORECAST_INPUT;
-- DROP STAGE IF EXISTS MY_CSV_STAGE;
-- DROP FILE FORMAT IF EXISTS MY_CSV_FORMAT;
-- DROP FILE FORMAT IF EXISTS MY_JSON_FORMAT;
-- DROP FILE FORMAT IF EXISTS MY_PARQUET_FORMAT;
-- DROP STREAM IF EXISTS SALES_CHANGES;
-- DROP STREAM IF EXISTS STREAM_DEMO_CDC;
-- DROP STREAM IF EXISTS SALES_APPEND_ONLY;
-- DROP DYNAMIC TABLE IF EXISTS YEARLY_REVENUE;
-- DROP DYNAMIC TABLE IF EXISTS QUARTERLY_REVENUE;
-- DROP DYNAMIC TABLE IF EXISTS MONTHLY_REVENUE_SUMMARY;
-- DROP SNOWFLAKE.ML.FORECAST IF EXISTS REVENUE_FORECAST_MODEL;
-- DROP SNOWFLAKE.ML.ANOMALY_DETECTION IF EXISTS REVENUE_ANOMALY_MODEL;
-- DROP SNOWFLAKE.ML.CLASSIFICATION IF EXISTS REVENUE_CLASSIFIER;
-- DROP FUNCTION IF EXISTS CLASSIFY_REVENUE(FLOAT);
-- DROP FUNCTION IF EXISTS CALC_MARGIN(FLOAT, FLOAT);
-- DROP FUNCTION IF EXISTS ZSCORE(FLOAT);
-- DROP FUNCTION IF EXISTS NORMALIZE_REVENUE(FLOAT, FLOAT, FLOAT);
-- DROP FUNCTION IF EXISTS PARSE_EMAIL_DOMAIN(VARCHAR);
-- DROP FUNCTION IF EXISTS SPLIT_TO_ROWS(VARCHAR, VARCHAR);
-- DROP FUNCTION IF EXISTS REVENUE_TIER(FLOAT);
