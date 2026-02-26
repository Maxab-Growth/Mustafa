-- =============================================================================
-- P70 RETAILERS BENCHMARK QUERY
-- =============================================================================
-- Addition to the existing PERFORMANCE_BENCHMARK_QUERY to add P70 retailers
-- This should be added after the p80_daily_benchmark CTE
-- Parameters:
--   {timezone} - Timezone for conversion
-- =============================================================================

-- ============================================================================
-- ADD THESE CTEs TO THE EXISTING PERFORMANCE_BENCHMARK_QUERY:
-- ============================================================================

-- Daily sales WITH RETAILER COUNT aggregation (240 days)
-- Replace the existing daily_sales CTE with this one:
/*
daily_sales AS (
    SELECT
        pso.warehouse_id,
        pso.product_id,
        so.created_at::DATE AS sale_date,
        SUM(pso.purchased_item_count * pso.basic_unit_count) AS daily_qty,
        COUNT(DISTINCT so.retailer_id) AS daily_retailers  -- NEW: Add retailer count
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    CROSS JOIN params p
    WHERE so.created_at::DATE >= p.history_start
        AND so.created_at::DATE < p.today
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
    GROUP BY pso.warehouse_id, pso.product_id, so.created_at::DATE
),

-- Update daily_with_stock to include retailers:
daily_with_stock AS (
    SELECT
        COALESCE(ds.warehouse_id, st.warehouse_id) AS warehouse_id,
        COALESCE(ds.product_id, st.product_id) AS product_id,
        COALESCE(ds.sale_date, st.stock_date) AS the_date,
        COALESCE(ds.daily_qty, 0) AS daily_qty,
        COALESCE(ds.daily_retailers, 0) AS daily_retailers,  -- NEW
        COALESCE(st.in_stock_flag, 0) AS in_stock_flag
    FROM daily_sales ds
    FULL OUTER JOIN daily_stock st 
        ON ds.warehouse_id = st.warehouse_id 
        AND ds.product_id = st.product_id 
        AND ds.sale_date = st.stock_date
    WHERE COALESCE(ds.sale_date, st.stock_date) >= (SELECT history_start FROM params)
),

-- NEW: Calculate P70 retailer benchmark
p70_retailer_benchmark AS (
    SELECT
        warehouse_id,
        product_id,
        PERCENTILE_CONT(0.7) WITHIN GROUP (ORDER BY daily_retailers) AS p70_daily_retailers_240d,
        AVG(daily_retailers) AS avg_daily_retailers_240d,
        STDDEV(daily_retailers) AS std_daily_retailers_240d
    FROM daily_with_stock
    CROSS JOIN params p
    WHERE in_stock_flag = 1
        AND the_date >= p.history_start
        AND the_date < p.today - 7  -- Exclude last 7 days from benchmark
    GROUP BY warehouse_id, product_id
),
*/

-- ============================================================================
-- UPDATE THE FINAL SELECT TO INCLUDE P70 RETAILERS:
-- ============================================================================
/*
SELECT
    ... existing columns ...,
    -- NEW: Add P70 retailer benchmarks
    COALESCE(pr.p70_daily_retailers_240d, 1) AS p70_daily_retailers_240d,
    COALESCE(pr.avg_daily_retailers_240d, 0) AS avg_daily_retailers_240d,
    COALESCE(pr.std_daily_retailers_240d, 0) AS std_daily_retailers_240d

FROM current_metrics cm
LEFT JOIN p80_daily_benchmark pb ON cm.warehouse_id = pb.warehouse_id AND cm.product_id = pb.product_id
LEFT JOIN p80_7d_benchmark p7 ON cm.warehouse_id = p7.warehouse_id AND cm.product_id = p7.product_id
LEFT JOIN p80_mtd_benchmark pm ON cm.warehouse_id = pm.warehouse_id AND cm.product_id = pm.product_id
LEFT JOIN p70_retailer_benchmark pr ON cm.warehouse_id = pr.warehouse_id AND cm.product_id = pr.product_id  -- NEW JOIN
WHERE cm.warehouse_id IN (1, 236, 337, 8, 339, 170, 501, 401, 703, 632, 797, 962)
*/


-- =============================================================================
-- COMPLETE STANDALONE QUERY FOR P70 RETAILERS (if running separately)
-- =============================================================================
WITH params AS (
    SELECT
        CONVERT_TIMEZONE('{timezone}', 'Africa/Cairo', CURRENT_TIMESTAMP())::DATE AS today,
        CONVERT_TIMEZONE('{timezone}', 'Africa/Cairo', CURRENT_TIMESTAMP())::DATE - 240 AS history_start
),

daily_sales AS (
    SELECT
        pso.warehouse_id,
        pso.product_id,
        so.created_at::DATE AS sale_date,
        COUNT(DISTINCT so.retailer_id) AS daily_retailers
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    CROSS JOIN params p
    WHERE so.created_at::DATE >= p.history_start
        AND so.created_at::DATE < p.today
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
    GROUP BY pso.warehouse_id, pso.product_id, so.created_at::DATE
),

daily_stock AS (
    SELECT
        sdc.warehouse_id,
        sdc.product_id,
        sdc.TIMESTAMP::DATE AS stock_date,
        CASE 
            WHEN LAG(sdc.available_stock, 1) OVER (
                    PARTITION BY sdc.warehouse_id, sdc.product_id ORDER BY sdc.TIMESTAMP::DATE
                 ) > 0 
                 AND sdc.available_stock > 0 
            THEN 1 
            ELSE 0 
        END AS in_stock_flag
    FROM materialized_views.stock_day_close sdc
    CROSS JOIN params p
    WHERE sdc.TIMESTAMP::DATE >= p.history_start - 1
        AND sdc.TIMESTAMP::DATE < p.today
),

daily_with_stock AS (
    SELECT
        COALESCE(ds.warehouse_id, st.warehouse_id) AS warehouse_id,
        COALESCE(ds.product_id, st.product_id) AS product_id,
        COALESCE(ds.sale_date, st.stock_date) AS the_date,
        COALESCE(ds.daily_retailers, 0) AS daily_retailers,
        COALESCE(st.in_stock_flag, 0) AS in_stock_flag
    FROM daily_sales ds
    FULL OUTER JOIN daily_stock st 
        ON ds.warehouse_id = st.warehouse_id 
        AND ds.product_id = st.product_id 
        AND ds.sale_date = st.stock_date
    WHERE COALESCE(ds.sale_date, st.stock_date) >= (SELECT history_start FROM params)
)

SELECT
    warehouse_id,
    product_id,
    PERCENTILE_CONT(0.7) WITHIN GROUP (ORDER BY daily_retailers) AS p70_daily_retailers_240d,
    AVG(daily_retailers) AS avg_daily_retailers_240d,
    STDDEV(daily_retailers) AS std_daily_retailers_240d,
    COUNT(*) AS days_with_data
FROM daily_with_stock
CROSS JOIN params p
WHERE in_stock_flag = 1
    AND the_date >= p.history_start
    AND the_date < p.today - 7
GROUP BY warehouse_id, product_id

