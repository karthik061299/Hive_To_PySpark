/***************************************************************************************************
-- Program Name  : top3_category_revenue_per_region_with_audit.hql
-- Description   : Hive ETL to compute Top 3 categories by revenue per region (last 12 months)
--                 with stepwise processing, version control, and control table-based audit logging.
-- Author        : Sasmon
-- Created On    : 2018-10-03
***************************************************************************************************/

-- ===============================================================================================
--  DEFINE RUNTIME VARIABLES
-- ===============================================================================================
SET hivevar:RUN_ID = uuid();
SET hivevar:LOAD_TS = current_timestamp();
SET hivevar:PROCESS_NAME = 'TOP3_CATEGORY_REVENUE_PER_REGION';

-- ===============================================================================================
--  LOG PROCESS START
-- ===============================================================================================
INSERT INTO TABLE process_control_log
SELECT 
    '${hivevar:PROCESS_NAME}' AS process_name,
    '${hivevar:RUN_ID}' AS run_id,
    'STARTED' AS status,
    current_timestamp() AS log_ts,
    'Process initiated' AS message;

-- ===============================================================================================
--  FILTER SALES FOR LAST 12 MONTHS
-- ===============================================================================================
DROP TABLE IF EXISTS tmp_filtered_sales;
CREATE TABLE tmp_filtered_sales AS
SELECT 
    s.order_id,
    s.product_id,
    p.category_id,
    r.region_id,
    s.order_date,
    CAST(s.quantity * s.price AS DECIMAL(10,2)) AS revenue
FROM sales s
JOIN products p ON (s.product_id = p.product_id)
JOIN regions r  ON (s.region_id = r.region_id)
WHERE s.order_date >= ADD_MONTHS(CAST(current_date AS DATE), -12);

-- ===============================================================================================
--  CALCULATE TOTAL AND AVERAGE REVENUE PER CATEGORY PER REGION
-- ===============================================================================================
DROP TABLE IF EXISTS tmp_category_revenue;
CREATE TABLE tmp_category_revenue AS
SELECT 
    region_id,
    category_id,
    SUM(revenue) AS total_revenue,
    AVG(revenue) AS avg_order_value
FROM tmp_filtered_sales
GROUP BY region_id, category_id;

-- ===============================================================================================
--  RANK CATEGORIES BY REVENUE WITHIN EACH REGION
-- ===============================================================================================
DROP TABLE IF EXISTS tmp_ranked_categories;
CREATE TABLE tmp_ranked_categories AS
SELECT 
    region_id,
    category_id,
    total_revenue,
    avg_order_value,
    RANK() OVER (PARTITION BY region_id ORDER BY total_revenue DESC) AS category_rank
FROM tmp_category_revenue;

-- ===============================================================================================
--  INSERT TOP 3 PER REGION INTO VERSIONED TABLE
-- ===============================================================================================
INSERT INTO TABLE top3_category_revenue_region_versions
SELECT 
    '${hivevar:RUN_ID}' AS run_id,
    region_id,
    category_id,
    total_revenue,
    avg_order_value,
    category_rank,
    '${hivevar:LOAD_TS}' AS load_ts
FROM tmp_ranked_categories
WHERE category_rank <= 3
ORDER BY region_id, category_rank;

-- ===============================================================================================
--  LOG SUCCESS (TO CONTROL TABLE)
-- ===============================================================================================
INSERT INTO TABLE process_control_log
SELECT 
    '${hivevar:PROCESS_NAME}' AS process_name,
    '${hivevar:RUN_ID}' AS run_id,
    'SUCCESS' AS status,
    current_timestamp() AS log_ts,
    'Data successfully inserted into versioned results table' AS message;
