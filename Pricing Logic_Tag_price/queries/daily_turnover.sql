-- =============================================================================
-- DAILY TURNOVER QUERY (WAREHOUSE LEVEL)
-- =============================================================================
-- This query calculates daily blended turnover at warehouse level
-- Turnover = Opening Stock / Qty Sold
-- 
-- Key Features:
-- - Aggregated at warehouse level (all products combined)
-- - Uses day close stocks and opening stocks (previous day's close)
-- - Opening stock = previous day's closing stock (summed across all products)
-- - Daily turnover = opening_stock / daily_qty_sold
-- - MTD turnover = MTD average opening stock / MTD average daily qty
-- - Shows both daily and MTD metrics
-- =============================================================================

WITH 
-- Step 1: Get Daily Closing Stocks from stock_day_close (aggregated by warehouse)
daily_closing_stocks AS (
    SELECT 
        sdc.warehouse_id,
        sdc.TIMESTAMP::DATE AS stock_date,
        SUM(sdc.available_stock) AS closing_stock
    FROM materialized_views.stock_day_close sdc
    WHERE sdc.TIMESTAMP::DATE >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
        AND sdc.TIMESTAMP::DATE < CURRENT_DATE
        AND sdc.warehouse_id IN (1, 8, 170, 236, 337, 339, 343, 401, 467, 501, 632, 703, 797, 962)
    GROUP BY sdc.warehouse_id, sdc.TIMESTAMP::DATE
),

-- Step 2: Calculate Opening Stocks (previous day's closing stock) at warehouse level
daily_stocks_with_opening AS (
    SELECT 
        warehouse_id,
        stock_date,
        closing_stock,
        LAG(closing_stock, 1) OVER (
            PARTITION BY warehouse_id 
            ORDER BY stock_date
        ) AS opening_stock
    FROM daily_closing_stocks
),

-- Step 3: Get Daily Quantity Sold (aggregated by warehouse)
daily_sales AS (
    SELECT 
        so.warehouse_id,
        so.created_at::DATE AS sale_date,
        SUM(pso.purchased_item_count * pso.basic_unit_count) AS daily_qty_sold
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    WHERE so.created_at::DATE >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
        AND so.created_at::DATE < CURRENT_DATE
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
    GROUP BY so.warehouse_id, so.created_at::DATE
),

-- Step 4: Combine Stocks and Sales at warehouse level
daily_data AS (
    SELECT 
        COALESCE(ds.warehouse_id, st.warehouse_id) AS warehouse_id,
        COALESCE(ds.sale_date, st.stock_date) AS the_date,
        COALESCE(st.opening_stock, 0) AS opening_stock,
        COALESCE(st.closing_stock, 0) AS closing_stock,
        COALESCE(ds.daily_qty_sold, 0) AS daily_qty_sold,
        -- Daily turnover = opening_stock / daily_qty_sold
        CASE 
            WHEN COALESCE(ds.daily_qty_sold, 0) > 0 
            THEN COALESCE(st.opening_stock, 0) / ds.daily_qty_sold
            ELSE NULL
        END AS daily_turnover
    FROM daily_stocks_with_opening st
    FULL OUTER JOIN daily_sales ds 
        ON st.warehouse_id = ds.warehouse_id 
        AND st.stock_date = ds.sale_date
    WHERE COALESCE(ds.sale_date, st.stock_date) >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
),

-- Step 5: Add Warehouse Information
daily_data_with_warehouse AS (
    SELECT 
        dd.*,
        w.name AS warehouse_name
    FROM daily_data dd
    JOIN warehouses w ON w.id = dd.warehouse_id
),

-- Step 6: Calculate MTD Metrics at warehouse level
mtd_metrics AS (
    SELECT 
        warehouse_id,
        -- MTD average opening stock
        AVG(opening_stock) AS mtd_avg_opening_stock,
        -- MTD total qty sold
        SUM(daily_qty_sold) AS mtd_total_qty,
        -- MTD number of days with sales
        COUNT(DISTINCT CASE WHEN daily_qty_sold > 0 THEN the_date END) AS mtd_selling_days,
        -- MTD average daily qty
        SUM(daily_qty_sold) / NULLIF(
            COUNT(DISTINCT CASE WHEN daily_qty_sold > 0 THEN the_date END), 0
        ) AS mtd_avg_daily_qty,
        -- MTD turnover = MTD avg opening stock / MTD avg daily qty
        CASE 
            WHEN SUM(daily_qty_sold) / NULLIF(
                COUNT(DISTINCT CASE WHEN daily_qty_sold > 0 THEN the_date END), 0
            ) > 0
            THEN AVG(opening_stock) / (
                SUM(daily_qty_sold) / NULLIF(
                    COUNT(DISTINCT CASE WHEN daily_qty_sold > 0 THEN the_date END), 0
                )
            )
            ELSE NULL
        END AS mtd_turnover
    FROM daily_data
    WHERE the_date >= DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY warehouse_id
),

-- Step 7: Get Current Month Start Date
current_month_start AS (
    SELECT DATE_TRUNC('month', CURRENT_DATE) AS month_start
)

-- Step 8: Final Output - Daily and MTD Turnover at Warehouse Level
SELECT 
    dd.warehouse_name,
    dd.warehouse_id,
    dd.the_date,
    
    -- Stock Information
    ROUND(dd.opening_stock, 0) AS opening_stock,
    ROUND(dd.closing_stock, 0) AS closing_stock,
    
    -- Sales Information
    ROUND(dd.daily_qty_sold, 0) AS daily_qty_sold,
    
    -- Daily Turnover
    ROUND(dd.daily_turnover, 2) AS daily_turnover,
    
    -- MTD Metrics
    ROUND(mtd.mtd_avg_opening_stock, 0) AS mtd_avg_opening_stock,
    ROUND(mtd.mtd_total_qty, 0) AS mtd_total_qty,
    mtd.mtd_selling_days,
    ROUND(mtd.mtd_avg_daily_qty, 2) AS mtd_avg_daily_qty,
    ROUND(mtd.mtd_turnover, 2) AS mtd_turnover,
    
    -- Additional Context
    CASE 
        WHEN dd.daily_turnover IS NULL THEN 'No Sales'
        WHEN dd.daily_turnover <= 7 THEN 'Fast Moving'
        WHEN dd.daily_turnover <= 14 THEN 'Normal'
        WHEN dd.daily_turnover <= 30 THEN 'Slow Moving'
        ELSE 'Very Slow'
    END AS daily_turnover_category,
    
    CASE 
        WHEN mtd.mtd_turnover IS NULL THEN 'No Sales'
        WHEN mtd.mtd_turnover <= 7 THEN 'Fast Moving'
        WHEN mtd.mtd_turnover <= 14 THEN 'Normal'
        WHEN mtd.mtd_turnover <= 30 THEN 'Slow Moving'
        ELSE 'Very Slow'
    END AS mtd_turnover_category

FROM daily_data_with_warehouse dd
LEFT JOIN mtd_metrics mtd 
    ON dd.warehouse_id = mtd.warehouse_id
WHERE dd.the_date >= (SELECT month_start FROM current_month_start)
ORDER BY 
    dd.the_date DESC,
    dd.warehouse_id;

