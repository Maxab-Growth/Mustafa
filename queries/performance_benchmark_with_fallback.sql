-- =============================================================================
-- PERFORMANCE BENCHMARK QUERY WITH FALLBACK LOGIC
-- =============================================================================
-- This query provides performance benchmarks (P80) for SKUs with fallback logic:
-- 1. Primary: p80_daily_240d (calculated from 240-day history)
-- 2. Fallback 1: Median cat-brand quantity (per warehouse)
-- 3. Fallback 2: Median cat quantity (per warehouse)
-- 4. Final fallback: 5
-- =============================================================================

WITH params AS (
    SELECT
        Current_timestamp::DATE AS today,
        Current_timestamp::DATE - 1 AS yesterday,
        Current_timestamp::DATE - 240 AS history_start,
        DATE_TRUNC('month', Current_timestamp::DATE) AS current_month_start,
        DAY(Current_timestamp::DATE) AS current_day_of_month
),

-- Product category and brand lookup
product_lookup AS (
    SELECT DISTINCT
        p.id AS product_id,
        b.name_ar AS brand,
        cat.name_ar AS cat
    FROM products p
    JOIN brands b ON b.id = p.brand_id
    JOIN categories cat ON cat.id = p.category_id
),

-- Daily sales aggregation (240 days) - includes qty and retailer count
daily_sales AS (
    SELECT
        pso.warehouse_id,
        pso.product_id,
        so.created_at::DATE AS sale_date,
        SUM(pso.purchased_item_count * pso.basic_unit_count) AS daily_qty,
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

-- Daily stock status using stock_day_close
-- In-stock = opening (prev day close) > 0 AND closing > 0
daily_stock AS (
    SELECT
        sdc.warehouse_id,
        sdc.product_id,
        sdc.TIMESTAMP::DATE AS stock_date,
        sdc.available_stock,
        LAG(sdc.available_stock, 1) OVER (
            PARTITION BY sdc.warehouse_id, sdc.product_id 
            ORDER BY sdc.TIMESTAMP::DATE
        ) AS opening_stock,
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
    WHERE sdc.TIMESTAMP::DATE >= p.history_start - 1  -- Need one extra day for LAG
        AND sdc.TIMESTAMP::DATE < p.today
),

-- Combine sales with stock status
daily_with_stock AS (
    SELECT
        COALESCE(ds.warehouse_id, st.warehouse_id) AS warehouse_id,
        COALESCE(ds.product_id, st.product_id) AS product_id,
        COALESCE(ds.sale_date, st.stock_date) AS the_date,
        COALESCE(ds.daily_qty, 0) AS daily_qty,
        COALESCE(ds.daily_retailers, 0) AS daily_retailers,
        COALESCE(st.in_stock_flag, 0) AS in_stock_flag
    FROM daily_sales ds
    FULL OUTER JOIN daily_stock st 
        ON ds.warehouse_id = st.warehouse_id 
        AND ds.product_id = st.product_id 
        AND ds.sale_date = st.stock_date
    WHERE COALESCE(ds.sale_date, st.stock_date) >= (SELECT history_start FROM params)
),

-- Add product category and brand to daily_with_stock for fallback calculations
daily_with_stock_cat_brand AS (
    SELECT
        dws.*,
        pl.brand,
        pl.cat
    FROM daily_with_stock dws
    LEFT JOIN product_lookup pl ON pl.product_id = dws.product_id
),

-- Identify new SKUs: those with sales ONLY in current month (no historical sales before current month)
new_sku_identification AS (
    SELECT
        warehouse_id,
        product_id,
        CASE 
            WHEN MAX(CASE WHEN the_date < (SELECT current_month_start FROM params) THEN 1 ELSE 0 END) = 0
                 AND MAX(CASE WHEN the_date >= (SELECT current_month_start FROM params) AND the_date < (SELECT today FROM params) THEN 1 ELSE 0 END) = 1
            THEN 1 
            ELSE 0 
        END AS is_new_sku
    FROM daily_with_stock
    WHERE daily_qty > 0  -- Only consider days with actual sales
    GROUP BY warehouse_id, product_id
),

-- Calculate P80 benchmark (in-stock days only, 240 days, EXCLUDING last 7 days)
p80_daily_benchmark AS (
    SELECT
        warehouse_id,
        product_id,
        PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY daily_qty) AS p80_daily_240d,
        AVG(daily_qty) AS avg_daily_240d,
        STDDEV(daily_qty) AS std_daily_240d,
        COUNT(*) AS in_stock_days_240d
    FROM daily_with_stock
    CROSS JOIN params p
    WHERE in_stock_flag = 1
        AND the_date >= p.history_start
        AND the_date < p.today - 7  -- Exclude last 7 days from benchmark
    GROUP BY warehouse_id, product_id
),

-- Calculate median cat-brand quantity (fallback 1)
-- Same filters: 240 days, in-stock only, exclude last 7 days, per warehouse
cat_brand_median AS (
    SELECT
        dws.warehouse_id,
        dws.cat,
        dws.brand,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dws.daily_qty) AS median_cat_brand_qty
    FROM daily_with_stock_cat_brand dws
    CROSS JOIN params p
    WHERE dws.in_stock_flag = 1
        AND dws.the_date >= p.history_start
        AND dws.the_date < p.today - 7  -- Exclude last 7 days from benchmark
        AND dws.cat IS NOT NULL
        AND dws.brand IS NOT NULL
    GROUP BY dws.warehouse_id, dws.cat, dws.brand
),

-- Calculate median cat quantity (fallback 2)
-- Same filters: 240 days, in-stock only, exclude last 7 days, per warehouse
cat_median AS (
    SELECT
        dws.warehouse_id,
        dws.cat,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dws.daily_qty) AS median_cat_qty
    FROM daily_with_stock_cat_brand dws
    CROSS JOIN params p
    WHERE dws.in_stock_flag = 1
        AND dws.the_date >= p.history_start
        AND dws.the_date < p.today - 7  -- Exclude last 7 days from benchmark
        AND dws.cat IS NOT NULL
    GROUP BY dws.warehouse_id, dws.cat
),

-- Calculate P70 retailer benchmark (in-stock days only, 240 days, EXCLUDING last 7 days)
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

-- NEW: Calculate current-month-only benchmarks for new SKUs
-- Use previous days of current month (excluding yesterday and last 1-2 days for stability)
current_month_benchmark AS (
    SELECT
        dws.warehouse_id,
        dws.product_id,
        PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY dws.daily_qty) AS p80_daily_current_month,
        AVG(dws.daily_qty) AS avg_daily_current_month,
        STDDEV(dws.daily_qty) AS std_daily_current_month,
        COUNT(*) AS in_stock_days_current_month
    FROM daily_with_stock dws
    CROSS JOIN params p
    INNER JOIN new_sku_identification nsi 
        ON dws.warehouse_id = nsi.warehouse_id 
        AND dws.product_id = nsi.product_id
    WHERE dws.in_stock_flag = 1
        AND dws.the_date >= p.current_month_start
        AND dws.the_date < p.yesterday - 1  -- Exclude yesterday and day before for stability
        AND nsi.is_new_sku = 1
    GROUP BY dws.warehouse_id, dws.product_id
    HAVING COUNT(*) >= 2  -- Need at least 2 days of data
),

-- NEW: Calculate current-month retailer benchmark for new SKUs
current_month_retailer_benchmark AS (
    SELECT
        dws.warehouse_id,
        dws.product_id,
        PERCENTILE_CONT(0.7) WITHIN GROUP (ORDER BY dws.daily_retailers) AS p70_daily_retailers_current_month,
        AVG(dws.daily_retailers) AS avg_daily_retailers_current_month,
        STDDEV(dws.daily_retailers) AS std_daily_retailers_current_month
    FROM daily_with_stock dws
    CROSS JOIN params p
    INNER JOIN new_sku_identification nsi 
        ON dws.warehouse_id = nsi.warehouse_id 
        AND dws.product_id = nsi.product_id
    WHERE dws.in_stock_flag = 1
        AND dws.the_date >= p.current_month_start
        AND dws.the_date < p.yesterday - 1  -- Exclude yesterday and day before
        AND nsi.is_new_sku = 1
    GROUP BY dws.warehouse_id, dws.product_id
    HAVING COUNT(*) >= 2
),

-- Calculate 7-day rolling SUM for P80 recent benchmark
rolling_7d AS (
    SELECT
        warehouse_id,
        product_id,
        the_date,
        SUM(daily_qty) OVER (
            PARTITION BY warehouse_id, product_id 
            ORDER BY the_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_sum,
        SUM(in_stock_flag) OVER (
            PARTITION BY warehouse_id, product_id 
            ORDER BY the_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS in_stock_days_7d
    FROM daily_with_stock
),

p80_7d_benchmark AS (
    SELECT
        warehouse_id,
        product_id,
        PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY rolling_7d_sum) AS p80_7d_rolling_240d
    FROM rolling_7d
    CROSS JOIN params p
    WHERE the_date >= p.history_start + 7  -- Need 7 days for rolling
        AND the_date < p.today - 7  -- Exclude last 7 days from benchmark
        AND in_stock_days_7d >= 4  -- At least 4 of 7 days in stock
    GROUP BY warehouse_id, product_id
),

-- NEW: Calculate 7-day rolling benchmark for new SKUs using current month data
current_month_7d_benchmark AS (
    SELECT
        r7d.warehouse_id,
        r7d.product_id,
        PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY r7d.rolling_7d_sum) AS p80_7d_rolling_current_month
    FROM rolling_7d r7d
    CROSS JOIN params p
    INNER JOIN new_sku_identification nsi 
        ON r7d.warehouse_id = nsi.warehouse_id 
        AND r7d.product_id = nsi.product_id
    WHERE r7d.the_date >= p.current_month_start + 7  -- Need 7 days for rolling
        AND r7d.the_date < p.yesterday - 1  -- Exclude last 1-2 days
        AND r7d.in_stock_days_7d >= 4  -- At least 4 of 7 days in stock
        AND nsi.is_new_sku = 1
    GROUP BY r7d.warehouse_id, r7d.product_id
),

-- MTD benchmark: P80 of same MTD period totals (last 12 months)
-- Sum all sales from day 1 to current day of month for each historical month
mtd_historical AS (
    SELECT
        dws.warehouse_id,
        dws.product_id,
        DATE_TRUNC('month', dws.the_date) AS period_month_start,
        SUM(dws.daily_qty) AS mtd_total_qty  -- Sum of all days from 1 to current_day_of_month
    FROM daily_with_stock dws
    CROSS JOIN params p
    WHERE DAY(dws.the_date) <= p.current_day_of_month  -- Only days up to current day of month
    GROUP BY dws.warehouse_id, dws.product_id, DATE_TRUNC('month', dws.the_date)
),

mtd_by_period AS (
    SELECT
        mh.warehouse_id,
        mh.product_id,
        mh.period_month_start,
        mh.mtd_total_qty AS mtd_qty_at_day  -- Total MTD qty for that month
    FROM mtd_historical mh
    CROSS JOIN params p
    WHERE mh.period_month_start >= DATEADD(month, -12, p.current_month_start)
        AND mh.period_month_start < p.current_month_start
),

p80_mtd_benchmark AS (
    SELECT
        warehouse_id,
        product_id,
        PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY mtd_qty_at_day) AS p80_mtd_12mo,
        AVG(mtd_qty_at_day) AS avg_mtd_12mo
    FROM mtd_by_period
    GROUP BY warehouse_id, product_id
    HAVING COUNT(*) >= 3  -- At least 3 months of data
),

-- Current period quantities
current_metrics AS (
    SELECT
        warehouse_id,
        product_id,
        -- Yesterday
        SUM(CASE WHEN the_date = (SELECT yesterday FROM params) THEN daily_qty ELSE 0 END) AS yesterday_qty,
        SUM(CASE WHEN the_date = (SELECT yesterday FROM params) THEN daily_retailers ELSE 0 END) AS yesterday_retailers,
        -- Yesterday in-stock flag (1 if in stock yesterday, 0 otherwise)
        MAX(CASE WHEN the_date = (SELECT yesterday FROM params) THEN in_stock_flag ELSE 0 END) AS yesterday_in_stock,
        -- Recent 7 days
        SUM(CASE WHEN the_date >= (SELECT today FROM params) - 7 AND the_date < (SELECT today FROM params) THEN daily_qty ELSE 0 END) AS recent_7d_qty,
        SUM(CASE WHEN the_date >= (SELECT today FROM params) - 7 AND the_date < (SELECT today FROM params) AND in_stock_flag = 1 THEN 1 ELSE 0 END) AS recent_7d_in_stock_days,
        -- MTD
        SUM(CASE WHEN the_date >= (SELECT current_month_start FROM params) AND the_date < (SELECT today FROM params) THEN daily_qty ELSE 0 END) AS mtd_qty,
        SUM(CASE WHEN the_date >= (SELECT current_month_start FROM params) AND the_date < (SELECT today FROM params) AND in_stock_flag = 1 THEN 1 ELSE 0 END) AS mtd_in_stock_days
    FROM daily_with_stock
    GROUP BY warehouse_id, product_id
),

-- Combined performance ratio calculation with dynamic weights and fallback logic
-- Priority: New SKU current month benchmarks > Historical benchmarks > Fallback (cat-brand/cat median/5)
combined_ratio_calc AS (
    SELECT
        cm.warehouse_id,
        cm.product_id,
        cm.yesterday_qty,
        cm.yesterday_retailers,
        cm.yesterday_in_stock,
        cm.recent_7d_qty,
        cm.recent_7d_in_stock_days,
        cm.mtd_qty,
        cm.mtd_in_stock_days,
        
        -- Check if this is a new SKU
        COALESCE(nsi.is_new_sku, 0) AS is_new_sku,
        
        -- Get product category and brand for fallback lookup
        pl.brand,
        pl.cat,
        
        -- Benchmark values: For new SKUs, use current month benchmarks first, then fallback
        -- For existing SKUs, use historical benchmarks with fallback
        COALESCE(
            -- If new SKU and has current month benchmark, use it
            CASE WHEN COALESCE(nsi.is_new_sku, 0) = 1 THEN cmb.p80_daily_current_month ELSE NULL END,
            -- Otherwise use historical benchmark
            pb.p80_daily_240d,
            -- Fallback chain
            cbm.median_cat_brand_qty,
            cm_median.median_cat_qty,
            5
        ) AS p80_daily_240d,
        
        -- Average, stddev, and in_stock_days: Use current month for new SKUs, historical for others
        COALESCE(
            CASE WHEN COALESCE(nsi.is_new_sku, 0) = 1 THEN cmb.avg_daily_current_month ELSE NULL END,
            pb.avg_daily_240d,
            0
        ) AS avg_daily_240d,
        
        COALESCE(
            CASE WHEN COALESCE(nsi.is_new_sku, 0) = 1 THEN cmb.std_daily_current_month ELSE NULL END,
            pb.std_daily_240d,
            0
        ) AS std_daily_240d,
        
        COALESCE(
            CASE WHEN COALESCE(nsi.is_new_sku, 0) = 1 THEN cmb.in_stock_days_current_month ELSE NULL END,
            pb.in_stock_days_240d,
            0
        ) AS in_stock_days_240d,
        
        -- For 7d: Use current month for new SKUs, historical for others, then fallback
        COALESCE(
            -- If new SKU and has current month 7d benchmark, use it
            CASE WHEN COALESCE(nsi.is_new_sku, 0) = 1 THEN cm7d.p80_7d_rolling_current_month ELSE NULL END,
            -- Otherwise use historical 7d benchmark
            p7.p80_7d_rolling_240d,
            -- Fallback: use daily benchmark * 7
            COALESCE(
                CASE WHEN COALESCE(nsi.is_new_sku, 0) = 1 THEN cmb.p80_daily_current_month ELSE NULL END,
                pb.p80_daily_240d,
                cbm.median_cat_brand_qty,
                cm_median.median_cat_qty,
                5
            ) * 7,
            35  -- 5 * 7 as final fallback
        ) AS p80_7d_sum_240d,
        
        -- For MTD: Use current month daily * days for new SKUs, historical MTD for others, then fallback
        COALESCE(
            -- If new SKU, use current month daily benchmark * days
            CASE WHEN COALESCE(nsi.is_new_sku, 0) = 1 
                 THEN cmb.p80_daily_current_month * (SELECT current_day_of_month FROM params)
                 ELSE NULL 
            END,
            -- Otherwise use historical MTD benchmark
            pm.p80_mtd_12mo,
            -- Fallback: use daily benchmark * days
            COALESCE(
                CASE WHEN COALESCE(nsi.is_new_sku, 0) = 1 THEN cmb.p80_daily_current_month ELSE NULL END,
                pb.p80_daily_240d,
                cbm.median_cat_brand_qty,
                cm_median.median_cat_qty,
                5
            ) * (SELECT current_day_of_month FROM params),
            5 * (SELECT current_day_of_month FROM params)  -- Final fallback
        ) AS p80_mtd_12mo,
        
        -- Retailer benchmarks: Use current month for new SKUs, historical for others
        COALESCE(
            CASE WHEN COALESCE(nsi.is_new_sku, 0) = 1 THEN cmrb.p70_daily_retailers_current_month ELSE NULL END,
            pr.p70_daily_retailers_240d,
            1
        ) AS p70_daily_retailers_240d,
        
        COALESCE(
            CASE WHEN COALESCE(nsi.is_new_sku, 0) = 1 THEN cmrb.avg_daily_retailers_current_month ELSE NULL END,
            pr.avg_daily_retailers_240d,
            0
        ) AS avg_daily_retailers_240d,
        
        COALESCE(
            CASE WHEN COALESCE(nsi.is_new_sku, 0) = 1 THEN cmrb.std_daily_retailers_current_month ELSE NULL END,
            pr.std_daily_retailers_240d,
            0
        ) AS std_daily_retailers_240d,
        
        -- Calculate base ratios (capped at 3) using benchmarks with new SKU priority
        LEAST(
            cm.yesterday_qty / NULLIF(
                COALESCE(
                    CASE WHEN COALESCE(nsi.is_new_sku, 0) = 1 THEN cmb.p80_daily_current_month ELSE NULL END,
                    pb.p80_daily_240d,
                    cbm.median_cat_brand_qty,
                    cm_median.median_cat_qty,
                    5
                ), 0
            ), 3
        ) AS yesterday_ratio_capped,
        
        LEAST(
            cm.recent_7d_qty / NULLIF(
                COALESCE(
                    CASE WHEN COALESCE(nsi.is_new_sku, 0) = 1 THEN cm7d.p80_7d_rolling_current_month ELSE NULL END,
                    p7.p80_7d_rolling_240d,
                    COALESCE(
                        CASE WHEN COALESCE(nsi.is_new_sku, 0) = 1 THEN cmb.p80_daily_current_month ELSE NULL END,
                        pb.p80_daily_240d,
                        cbm.median_cat_brand_qty,
                        cm_median.median_cat_qty,
                        5
                    ) * 7,
                    35
                ), 0
            ), 3
        ) AS recent_ratio_capped,
        
        LEAST(
            cm.mtd_qty / NULLIF(
                COALESCE(
                    CASE WHEN COALESCE(nsi.is_new_sku, 0) = 1 
                         THEN cmb.p80_daily_current_month * (SELECT current_day_of_month FROM params)
                         ELSE NULL 
                    END,
                    pm.p80_mtd_12mo,
                    COALESCE(
                        CASE WHEN COALESCE(nsi.is_new_sku, 0) = 1 THEN cmb.p80_daily_current_month ELSE NULL END,
                        pb.p80_daily_240d,
                        cbm.median_cat_brand_qty,
                        cm_median.median_cat_qty,
                        5
                    ) * (SELECT current_day_of_month FROM params),
                    5 * (SELECT current_day_of_month FROM params)
                ), 0
            ), 3
        ) AS mtd_ratio_capped,
        
        -- In-stock percentages for each period
        cm.yesterday_in_stock AS yesterday_in_stock_pct,
        cm.recent_7d_in_stock_days / 7.0 AS recent_7d_in_stock_pct,
        cm.mtd_in_stock_days / NULLIF((SELECT current_day_of_month FROM params) - 1, 0) AS mtd_in_stock_pct,
        
        -- Raw weights (base: 20% yesterday, 40% recent 7d, 40% MTD) scaled by in-stock percentage
        0.2 * cm.yesterday_in_stock AS yesterday_raw_weight,
        0.4 * (cm.recent_7d_in_stock_days / 7.0) AS recent_7d_raw_weight,
        CASE 
            WHEN (SELECT current_day_of_month FROM params) >= 3 
            THEN 0.4 * COALESCE(cm.mtd_in_stock_days / NULLIF((SELECT current_day_of_month FROM params) - 1, 0), 0)
            ELSE 0 
        END AS mtd_raw_weight
        
    FROM current_metrics cm
    LEFT JOIN new_sku_identification nsi ON cm.warehouse_id = nsi.warehouse_id AND cm.product_id = nsi.product_id
    LEFT JOIN p80_daily_benchmark pb ON cm.warehouse_id = pb.warehouse_id AND cm.product_id = pb.product_id
    LEFT JOIN current_month_benchmark cmb ON cm.warehouse_id = cmb.warehouse_id AND cm.product_id = cmb.product_id
    LEFT JOIN p80_7d_benchmark p7 ON cm.warehouse_id = p7.warehouse_id AND cm.product_id = p7.product_id
    LEFT JOIN current_month_7d_benchmark cm7d ON cm.warehouse_id = cm7d.warehouse_id AND cm.product_id = cm7d.product_id
    LEFT JOIN p80_mtd_benchmark pm ON cm.warehouse_id = pm.warehouse_id AND cm.product_id = pm.product_id
    LEFT JOIN p70_retailer_benchmark pr ON cm.warehouse_id = pr.warehouse_id AND cm.product_id = pr.product_id
    LEFT JOIN current_month_retailer_benchmark cmrb ON cm.warehouse_id = cmrb.warehouse_id AND cm.product_id = cmrb.product_id
    LEFT JOIN product_lookup pl ON pl.product_id = cm.product_id
    LEFT JOIN cat_brand_median cbm ON cbm.warehouse_id = cm.warehouse_id 
        AND cbm.cat = pl.cat 
        AND cbm.brand = pl.brand
    LEFT JOIN cat_median cm_median ON cm_median.warehouse_id = cm.warehouse_id 
        AND cm_median.cat = pl.cat
),

-- Pre-calculate combined ratio to avoid repetition
final_with_combined AS (
    SELECT
        crc.*,
        -- Calculate combined performance ratio once
        CASE WHEN (crc.yesterday_raw_weight + crc.recent_7d_raw_weight + crc.mtd_raw_weight) > 0
        THEN (
            (crc.yesterday_raw_weight / (crc.yesterday_raw_weight + crc.recent_7d_raw_weight + crc.mtd_raw_weight)) * crc.yesterday_ratio_capped +
            (crc.recent_7d_raw_weight / (crc.yesterday_raw_weight + crc.recent_7d_raw_weight + crc.mtd_raw_weight)) * crc.recent_ratio_capped +
            (crc.mtd_raw_weight / (crc.yesterday_raw_weight + crc.recent_7d_raw_weight + crc.mtd_raw_weight)) * crc.mtd_ratio_capped
        )
        ELSE 0 END AS combined_perf_ratio
    FROM combined_ratio_calc crc
)

-- Final output (same columns as original, values adjusted when over-achieving)
SELECT
    f.warehouse_id,
    f.product_id,
    
    -- Current period quantities
    f.yesterday_qty,
    f.yesterday_retailers,
    f.recent_7d_qty,
    f.recent_7d_in_stock_days,
    f.mtd_qty,
    f.mtd_in_stock_days,
    
    -- Quantity Benchmarks (P80) - adjusted when over-achieving, with fallback already applied
    CASE WHEN f.combined_perf_ratio > 1.1 
         THEN ROUND(f.p80_daily_240d + 0.5 * f.std_daily_240d, 2)
         ELSE f.p80_daily_240d 
    END AS p80_daily_240d,
    f.avg_daily_240d,
    f.std_daily_240d,
    f.in_stock_days_240d,
    CASE WHEN f.combined_perf_ratio > 1.1 
         THEN ROUND(f.p80_7d_sum_240d + 0.5 * f.std_daily_240d * 7, 2)
         ELSE f.p80_7d_sum_240d 
    END AS p80_7d_sum_240d,
    CASE WHEN f.combined_perf_ratio > 1.1 
         THEN ROUND(f.p80_mtd_12mo + 0.5 * f.std_daily_240d * (SELECT current_day_of_month FROM params), 2)
         ELSE f.p80_mtd_12mo 
    END AS p80_mtd_12mo,
    
    -- Retailer Benchmarks (P70) - adjusted when over-achieving
    CASE WHEN f.combined_perf_ratio > 1.1 
         THEN ROUND(f.p70_daily_retailers_240d + 0.5 * f.std_daily_retailers_240d, 2)
         ELSE f.p70_daily_retailers_240d 
    END AS p70_daily_retailers_240d,
    f.avg_daily_retailers_240d,
    f.std_daily_retailers_240d,
    
    -- Performance ratios (adjusted when over-achieving)
    ROUND(f.yesterday_qty / NULLIF(
        CASE WHEN f.combined_perf_ratio > 1.1 
             THEN f.p80_daily_240d + 0.5 * f.std_daily_240d
             ELSE f.p80_daily_240d 
        END, 0), 2) AS yesterday_ratio,
    ROUND(f.recent_7d_qty / NULLIF(
        CASE WHEN f.combined_perf_ratio > 1.1 
             THEN f.p80_7d_sum_240d + 0.5 * f.std_daily_240d * 7
             ELSE f.p80_7d_sum_240d 
        END, 0), 2) AS recent_ratio,
    ROUND(f.mtd_qty / NULLIF(
        CASE WHEN f.combined_perf_ratio > 1.1 
             THEN f.p80_mtd_12mo + 0.5 * f.std_daily_240d * (SELECT current_day_of_month FROM params)
             ELSE f.p80_mtd_12mo 
        END, 0), 2) AS mtd_ratio,
    ROUND(f.yesterday_retailers / NULLIF(
        CASE WHEN f.combined_perf_ratio > 1.1 
             THEN f.p70_daily_retailers_240d + 0.5 * f.std_daily_retailers_240d
             ELSE f.p70_daily_retailers_240d 
        END, 0), 2) AS yesterday_retailer_ratio,
    
    -- Additional columns for visibility
    ROUND(f.combined_perf_ratio, 2) AS combined_perf_ratio,
    CASE WHEN f.combined_perf_ratio > 1.1 THEN 1 ELSE 0 END AS is_over_achiever,
    f.is_new_sku  -- Flag to identify new SKUs using current month benchmarks

FROM final_with_combined f
WHERE f.warehouse_id IN (1, 236, 337, 8, 339, 170, 501, 401, 703, 632, 797, 962);

