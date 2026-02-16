-- =============================================================================
-- CATEGORY-BRAND TURNOVER QUERY
-- =============================================================================
-- This query calculates daily turnover per category-brand combination
-- Shows top category-brands based on stock value (stocks * wac1)
-- Turnover = Opening Stock / Qty Sold
-- 
-- Key Features:
-- - Aggregated at category-brand level (not warehouse level)
-- - Ranks category-brands by stock value (stocks * wac1)
-- - Uses day close stocks and opening stocks (previous day's close)
-- - Opening stock = previous day's closing stock (summed across all products/warehouses)
-- - Daily turnover = opening_stock / daily_qty_sold
-- - MTD turnover = MTD average opening stock / MTD average daily qty
-- - Shows both daily and MTD metrics for top category-brands
-- =============================================================================

WITH 
-- Step 1: Get Daily Closing Stocks from stock_day_close at SKU (product) level first
daily_closing_stocks_sku AS (
    SELECT 
        sdc.product_id,
        sdc.TIMESTAMP::DATE AS stock_date,
        SUM(sdc.available_stock) AS closing_stock
    FROM materialized_views.stock_day_close sdc
    WHERE sdc.TIMESTAMP::DATE >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
        AND sdc.TIMESTAMP::DATE < CURRENT_DATE
        AND sdc.warehouse_id IN (1, 8, 170, 236, 337, 339, 343, 401, 467, 501, 632, 703, 797, 962)
    GROUP BY sdc.product_id, sdc.TIMESTAMP::DATE
),

-- Step 2: Calculate Opening Stocks (previous day's closing stock) at SKU level
daily_stocks_with_opening_sku AS (
    SELECT 
        product_id,
        stock_date,
        closing_stock,
        LAG(closing_stock, 1) OVER (
            PARTITION BY product_id 
            ORDER BY stock_date
        ) AS opening_stock
    FROM daily_closing_stocks_sku
),

-- Step 3: Aggregate stocks to category-brand level
daily_closing_stocks AS (
    SELECT 
        cat.name_ar AS category,
        b.name_ar AS brand,
        dso.stock_date,
        SUM(dso.closing_stock) AS closing_stock,
        SUM(dso.opening_stock) AS opening_stock
    FROM daily_stocks_with_opening_sku dso
    JOIN products p ON p.id = dso.product_id
    JOIN brands b ON b.id = p.brand_id
    JOIN categories cat ON cat.id = p.category_id
    GROUP BY cat.name_ar, b.name_ar, dso.stock_date
),

-- Step 4: Get Daily Quantity Sold (aggregated by category-brand)
daily_sales AS (
    SELECT 
        cat.name_ar AS category,
        b.name_ar AS brand,
        so.created_at::DATE AS sale_date,
        SUM(pso.purchased_item_count * pso.basic_unit_count) AS daily_qty_sold
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    JOIN products p ON p.id = pso.product_id
    JOIN brands b ON b.id = p.brand_id
    JOIN categories cat ON cat.id = p.category_id
    WHERE so.created_at::DATE >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
        AND so.created_at::DATE < CURRENT_DATE
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
    GROUP BY cat.name_ar, b.name_ar, so.created_at::DATE
),

-- Step 5: Combine Stocks and Sales at category-brand level
daily_data AS (
    SELECT 
        COALESCE(ds.category, st.category) AS category,
        COALESCE(ds.brand, st.brand) AS brand,
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
    FROM daily_closing_stocks st
    FULL OUTER JOIN daily_sales ds 
        ON st.category = ds.category 
        AND st.brand = ds.brand
        AND st.stock_date = ds.sale_date
    WHERE COALESCE(ds.sale_date, st.stock_date) >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
),

-- Step 6: Calculate Current Stock Value at SKU level first (stocks * wac1 per SKU)
current_stock_value_sku AS (
    SELECT 
        sdc.product_id,
        SUM(sdc.available_stock) AS total_stock,
        SUM(sdc.available_stock * COALESCE(f.wac1, 0)) AS stock_value
    FROM materialized_views.stock_day_close sdc
    JOIN finance.all_cogs f ON f.product_id = sdc.product_id
        AND sdc.TIMESTAMP::DATE >= f.from_date::DATE
        AND sdc.TIMESTAMP::DATE < f.to_date::DATE
    WHERE sdc.TIMESTAMP::DATE = CURRENT_DATE - 1
        AND sdc.warehouse_id IN (1, 8, 170, 236, 337, 339, 343, 401, 467, 501, 632, 703, 797, 962)
        AND f.wac1 > 0
    GROUP BY sdc.product_id
),

-- Step 7: Aggregate stock value to category-brand level
current_stock_value AS (
    SELECT 
        cat.name_ar AS category,
        b.name_ar AS brand,
        SUM(csv.stock_value) AS stock_value
    FROM current_stock_value_sku csv
    JOIN products p ON p.id = csv.product_id
    JOIN brands b ON b.id = p.brand_id
    JOIN categories cat ON cat.id = p.category_id
    GROUP BY cat.name_ar, b.name_ar
),

-- Step 8: Rank category-brands by stock value
ranked_cat_brands AS (
    SELECT 
        category,
        brand,
        stock_value,
        ROW_NUMBER() OVER (ORDER BY stock_value DESC) AS rank_by_stock_value
    FROM current_stock_value
    WHERE stock_value > 0
),

-- Step 9: Calculate MTD Metrics at category-brand level
mtd_metrics AS (
    SELECT 
        category,
        brand,
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
    GROUP BY category, brand
),

-- Step 10: Get Current Month Start Date
current_month_start AS (
    SELECT DATE_TRUNC('month', CURRENT_DATE) AS month_start
),

-- Step 11: Filter to top category-brands and combine all data
top_cat_brand_data AS (
    SELECT 
        dd.category,
        dd.brand,
        dd.the_date,
        rcb.stock_value,
        rcb.rank_by_stock_value,
        dd.opening_stock,
        dd.closing_stock,
        dd.daily_qty_sold,
        dd.daily_turnover,
        mtd.mtd_avg_opening_stock,
        mtd.mtd_total_qty,
        mtd.mtd_selling_days,
        mtd.mtd_avg_daily_qty,
        mtd.mtd_turnover
    FROM daily_data dd
    INNER JOIN ranked_cat_brands rcb 
        ON dd.category = rcb.category 
        AND dd.brand = rcb.brand
    LEFT JOIN mtd_metrics mtd 
        ON dd.category = mtd.category 
        AND dd.brand = mtd.brand
    WHERE rcb.rank_by_stock_value <= 50  -- Top 50 category-brands by stock value
)

-- Step 12: Final Output - Daily and MTD Turnover for Top Category-Brands
SELECT 
    tcb.category,
    tcb.brand,
    tcb.rank_by_stock_value,
    ROUND(tcb.stock_value, 0) AS stock_value,
    tcb.the_date,
    
    -- Stock Information
    ROUND(tcb.opening_stock, 0) AS opening_stock,
    ROUND(tcb.closing_stock, 0) AS closing_stock,
    
    -- Sales Information
    ROUND(tcb.daily_qty_sold, 0) AS daily_qty_sold,
    
    -- Daily Turnover
    ROUND(tcb.daily_turnover, 2) AS daily_turnover,
    
    -- MTD Metrics
    ROUND(tcb.mtd_avg_opening_stock, 0) AS mtd_avg_opening_stock,
    ROUND(tcb.mtd_total_qty, 0) AS mtd_total_qty,
    tcb.mtd_selling_days,
    ROUND(tcb.mtd_avg_daily_qty, 2) AS mtd_avg_daily_qty,
    ROUND(tcb.mtd_turnover, 2) AS mtd_turnover,
    
    -- Additional Context
    CASE 
        WHEN tcb.daily_turnover IS NULL THEN 'No Sales'
        WHEN tcb.daily_turnover <= 7 THEN 'Fast Moving'
        WHEN tcb.daily_turnover <= 14 THEN 'Normal'
        WHEN tcb.daily_turnover <= 30 THEN 'Slow Moving'
        ELSE 'Very Slow'
    END AS daily_turnover_category,
    
    CASE 
        WHEN tcb.mtd_turnover IS NULL THEN 'No Sales'
        WHEN tcb.mtd_turnover <= 7 THEN 'Fast Moving'
        WHEN tcb.mtd_turnover <= 14 THEN 'Normal'
        WHEN tcb.mtd_turnover <= 30 THEN 'Slow Moving'
        ELSE 'Very Slow'
    END AS mtd_turnover_category

FROM top_cat_brand_data tcb
WHERE tcb.the_date >= (SELECT month_start FROM current_month_start)
ORDER BY 
    tcb.rank_by_stock_value,
    tcb.the_date DESC;

