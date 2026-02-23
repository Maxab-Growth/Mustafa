-- =============================================================================
-- UTH ACHIEVEMENT QUERY
-- =============================================================================
-- This query calculates UTH (Up-Till-Hour) targets and achievement percentages
-- for in-stock products from Pricing_data_extraction.
--
-- Key Calculations:
-- 1. UTH Target = p_80_daily * uth_contribution (cat-level from last month)
-- 2. Actual Running Rates = Today's sales up to current hour
-- 3. Achievement Percentage = (actual_qty_uth / uth_target) * 100
-- 4. Effective Price = Price after both SKU discount and QD discount
-- 5. Stock Value = available_stock * current_price
--
-- Purpose: Monitor daily performance against UTH targets with pricing context
-- =============================================================================

WITH 
params AS (
    SELECT
        CURRENT_DATE AS today,
        EXTRACT(HOUR FROM CURRENT_TIMESTAMP()) AS current_hour,
        GREATEST(EXTRACT(HOUR FROM CURRENT_TIMESTAMP()) - 4, 0) AS hour_4h_ago,  -- 4 hours ago (minimum 0)
        DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AS last_month_start,
        DATE_TRUNC('month', CURRENT_DATE) AS last_month_end
),

-- Step 1: Get Base Data from Pricing_data_extraction (in-stock products only)
base_data AS (
    SELECT 
        pd.*,
        pw.available_stock
    FROM MATERIALIZED_VIEWS.Pricing_data_extraction pd
    JOIN product_warehouse pw 
        ON pw.product_id = pd.product_id 
        AND pw.warehouse_id = pd.warehouse_id
        AND pw.is_basic_unit = 1
    JOIN products p ON p.id = pd.product_id
    WHERE pd.created_at::date = CURRENT_DATE
        AND pw.available_stock > 0  -- Only in-stock products
),

-- Step 2: Calculate UTH Contribution (Cat-Warehouse Level, Last Month)
uth_contribution AS (
    WITH hourly_sales_hist AS (
        -- Get historical hourly sales by category-warehouse for last month
        SELECT 
            so.warehouse_id,
            c.name_ar AS cat,
            so.created_at::date AS sale_date,
            EXTRACT(HOUR FROM so.created_at) AS sale_hour,
            SUM(pso.purchased_item_count * pso.basic_unit_count) AS qty
        FROM product_sales_order pso
        JOIN sales_orders so ON so.id = pso.sales_order_id
        JOIN products p ON p.id = pso.product_id
        JOIN categories c ON c.id = p.category_id
        CROSS JOIN params par
        WHERE so.created_at::date >= par.last_month_start
            AND so.created_at::date < par.last_month_end
            AND so.sales_order_status_id NOT IN (7, 12)
            AND so.channel IN ('telesales', 'retailer')
            AND pso.purchased_item_count <> 0
        GROUP BY so.warehouse_id, c.name_ar, so.created_at::date, EXTRACT(HOUR FROM so.created_at)
    ),
    daily_totals_hist AS (
        -- Calculate daily totals and UTH totals from historical data
        SELECT 
            warehouse_id,
            cat,
            sale_date,
            SUM(qty) AS day_total_qty,
            SUM(CASE WHEN sale_hour < (SELECT current_hour FROM params) THEN qty ELSE 0 END) AS uth_total_qty
        FROM hourly_sales_hist
        GROUP BY warehouse_id, cat, sale_date
    )
    -- Get average UTH percentage by category-warehouse
    SELECT 
        warehouse_id,
        cat,
        AVG(COALESCE(uth_total_qty, 0) / NULLIF(day_total_qty, 0)) AS uth_contribution
    FROM daily_totals_hist
    WHERE day_total_qty > 0
    GROUP BY warehouse_id, cat
),

-- Step 3: Calculate UTH Target (p_80_daily * uth_contribution)
uth_targets AS (
    SELECT 
        bd.product_id,
        bd.warehouse_id,
        bd.p80_daily_240d,
        COALESCE(uc.uth_contribution, 0.5) AS uth_contribution,  -- Default to 0.5 if no historical data
        bd.p80_daily_240d * COALESCE(uc.uth_contribution, 0.5) AS uth_target
    FROM base_data bd
    LEFT JOIN uth_contribution uc
        ON uc.warehouse_id = bd.warehouse_id 
        AND uc.cat = bd.cat
),

-- Step 4: Calculate Actual Running Rates (Today UTH)
actual_uth AS (
    SELECT 
        so.warehouse_id,
        pso.product_id,
        SUM(pso.purchased_item_count * pso.basic_unit_count) AS actual_qty_uth
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    CROSS JOIN params par
    WHERE so.created_at::date = par.today
        AND EXTRACT(HOUR FROM so.created_at) <= par.current_hour
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
    GROUP BY so.warehouse_id, pso.product_id
),

-- Step 5: Get Current Price (from Pricing_data_extraction if available, else from cohort_product_packing_units)
current_prices AS (
    WITH prices_from_pde AS (
        -- Get from Pricing_data_extraction if available
        SELECT DISTINCT
            bd.product_id,
            bd.warehouse_id,
            bd.current_price
        FROM base_data bd
        WHERE bd.current_price IS NOT NULL
    ),
    prices_from_cohort AS (
        -- Fallback: Get from cohort_product_packing_units with warehouse mapping
        SELECT DISTINCT
            pu.product_id,
            cwm.warehouse_id,
            AVG(cpu.price) AS current_price
        FROM cohort_product_packing_units cpu
        JOIN PACKING_UNIT_PRODUCTS pu ON pu.id = cpu.product_packing_unit_id
        JOIN (
            SELECT * FROM VALUES
            (700,1), (701,236), (701,962), (702,797), (703,337), (703,8),
            (704,339), (704,170), (1123,703), (1124,501), (1125,632), (1126,401)
            AS x(cohort_id, warehouse_id)
        ) cwm ON cwm.cohort_id = cpu.cohort_id
        WHERE cpu.cohort_id IN (700,701,702,703,704,695,696,697,698,699,1123,1124,1125,1126)
            AND cpu.created_at::date <> '2023-07-31'
            AND cpu.is_customized = TRUE
            AND pu.basic_unit_count = 1
            AND ((pu.product_id = 1309 AND pu.packing_unit_id = 2) OR (pu.product_id <> 1309))
        GROUP BY pu.product_id, cwm.warehouse_id
    )
    -- Prioritize Pricing_data_extraction price, fallback to cohort price
    SELECT 
        COALESCE(pde.product_id, pc.product_id) AS product_id,
        COALESCE(pde.warehouse_id, pc.warehouse_id) AS warehouse_id,
        COALESCE(pc.current_price,pde.current_price) AS current_price
    FROM prices_from_pde pde
    FULL OUTER JOIN prices_from_cohort pc
        ON pde.product_id = pc.product_id 
        AND pde.warehouse_id = pc.warehouse_id
),

-- Step 6: Get Effective Price (Actual from Sales - After Both SKU and QD Discounts)
effective_prices AS (
    SELECT 
        pso.warehouse_id,
        pso.product_id,
        -- Effective price = (total_price - SKU_discount - QD_discount) / (qty * basic_unit_count)
        AVG(
            (pso.total_price 
             - COALESCE(pso.ITEM_DISCOUNT_VALUE * pso.purchased_item_count, 0)
             - COALESCE(pso.ITEM_QUANTITY_DISCOUNT_VALUE * pso.purchased_item_count, 0)
            ) / NULLIF(pso.purchased_item_count * pso.basic_unit_count, 0)
        ) AS effective_price
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    WHERE so.created_at > CURRENT_DATE 
        AND so.created_at <= CURRENT_TIMESTAMP()
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
    GROUP BY pso.warehouse_id, pso.product_id
),

-- Step 7: Check for Active SKU Discounts
active_sku_discounts AS (
    WITH active_sku_discount AS (
        SELECT 
            sd.id AS sku_discount_id,
            f.value::INT AS retailer_id,
            sd.start_at,
            sd.end_at
        FROM SKU_DISCOUNTS sd,
        LATERAL FLATTEN(
            input => SPLIT(
                REPLACE(REPLACE(REPLACE(sd.retailer_ids, '{', ''), '}', ''), '"', ''), 
                ','
            )
        ) f
        WHERE sd.name_en = 'Special Discounts'
            AND sd.active = 'true'
            AND sd.start_at <= CURRENT_TIMESTAMP()
            AND sd.end_at >= CURRENT_TIMESTAMP()
    ),
    sku_discount_warehouse_mapping AS (
        SELECT DISTINCT
            sdv.product_id,
            wdr.warehouse_id
        FROM active_sku_discount asd
        JOIN SKU_DISCOUNT_VALUES sdv ON sdv.sku_discount_id = asd.sku_discount_id
        JOIN materialized_views.retailer_polygon rp ON rp.retailer_id = asd.retailer_id
        JOIN WAREHOUSE_DISPATCHING_RULES wdr ON wdr.product_id = sdv.product_id
        JOIN DISPATCHING_POLYGONS dp ON dp.id = wdr.DISPATCHING_POLYGON_ID 
            AND dp.district_id = rp.district_id
    )
    SELECT DISTINCT
        product_id,
        warehouse_id,
        1 AS has_sku_discount
    FROM sku_discount_warehouse_mapping
),

-- Step 8: Check for Active Quantity Discounts
active_qd_discounts AS (
    WITH qd_det AS (
        SELECT DISTINCT 
            dt.id AS tag_id, 
            dt.name AS tag_name,
            REPLACE(w.name, ' ', '') AS warehouse_name,
            w.id AS warehouse_id,
            warehouse_name ILIKE '%' || CASE 
                WHEN SPLIT_PART(tag_name, '_', 1) = 'El' THEN SPLIT_PART(tag_name, '_', 2) 
                ELSE SPLIT_PART(tag_name, '_', 1) 
            END || '%' AS contains_flag
        FROM dynamic_tags dt
        JOIN dynamic_taggables dta ON dt.id = dta.dynamic_tag_id 
        CROSS JOIN warehouses w 
        WHERE dt.id > 3000
            AND dt.name LIKE '%QD_rets%'
            AND w.id IN (1, 236, 337, 8, 339, 170, 501, 401, 703, 632, 797, 962)
            AND contains_flag = 'true'
    )
    SELECT DISTINCT
        qdv.product_id,
        qd_det.warehouse_id,
        1 AS has_qty_discount
    FROM quantity_discounts qd 
    JOIN quantity_discount_values qdv ON qdv.quantity_discount_id = qd.id
    JOIN qd_det ON qd_det.tag_id = qd.dynamic_tag_id
    WHERE qd.active = TRUE
        AND qd.start_at <= CURRENT_TIMESTAMP()
        AND qd.end_at >= CURRENT_TIMESTAMP()
),

-- Step 9: Calculate UTH Contribution for 4 Hours Ago (Cat-Warehouse Level, Last Month)
uth_contribution_4h_ago AS (
    WITH hourly_sales_hist AS (
        -- Get historical hourly sales by category-warehouse for last month
        SELECT 
            so.warehouse_id,
            c.name_ar AS cat,
            so.created_at::date AS sale_date,
            EXTRACT(HOUR FROM so.created_at) AS sale_hour,
            SUM(pso.purchased_item_count * pso.basic_unit_count) AS qty
        FROM product_sales_order pso
        JOIN sales_orders so ON so.id = pso.sales_order_id
        JOIN products p ON p.id = pso.product_id
        JOIN categories c ON c.id = p.category_id
        CROSS JOIN params par
        WHERE so.created_at::date >= par.last_month_start
            AND so.created_at::date < par.last_month_end
            AND so.sales_order_status_id NOT IN (7, 12)
            AND so.channel IN ('telesales', 'retailer')
            AND pso.purchased_item_count <> 0
        GROUP BY so.warehouse_id, c.name_ar, so.created_at::date, EXTRACT(HOUR FROM so.created_at)
    ),
    daily_totals_hist AS (
        -- Calculate daily totals and UTH totals from historical data (for 4 hours ago)
        SELECT 
            warehouse_id,
            cat,
            sale_date,
            SUM(qty) AS day_total_qty,
            SUM(CASE WHEN sale_hour < (SELECT hour_4h_ago FROM params) THEN qty ELSE 0 END) AS uth_total_qty
        FROM hourly_sales_hist
        GROUP BY warehouse_id, cat, sale_date
    )
    -- Get average UTH percentage by category-warehouse for 4 hours ago
    SELECT 
        warehouse_id,
        cat,
        AVG(COALESCE(uth_total_qty, 0) / NULLIF(day_total_qty, 0)) AS uth_contribution_4h_ago
    FROM daily_totals_hist
    WHERE day_total_qty > 0
    GROUP BY warehouse_id, cat
),

-- Step 10: Calculate UTH Target for 4 Hours Ago (p_80_daily * uth_contribution_4h_ago)
uth_targets_4h_ago AS (
    SELECT 
        bd.product_id,
        bd.warehouse_id,
        bd.p80_daily_240d,
        COALESCE(uc4.uth_contribution_4h_ago, 0.5) AS uth_contribution_4h_ago,  -- Default to 0.5 if no historical data
        bd.p80_daily_240d * COALESCE(uc4.uth_contribution_4h_ago, 0.5) AS uth_target_4h_ago
    FROM base_data bd
    LEFT JOIN uth_contribution_4h_ago uc4
        ON uc4.warehouse_id = bd.warehouse_id 
        AND uc4.cat = bd.cat
),

-- Step 11: Calculate Actual Running Rates for 4 Hours Ago (Today UTH up to hour_4h_ago)
actual_uth_4h_ago AS (
    SELECT 
        so.warehouse_id,
        pso.product_id,
        SUM(pso.purchased_item_count * pso.basic_unit_count) AS actual_qty_uth_4h_ago
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    CROSS JOIN params par
    WHERE so.created_at::date = par.today
        AND EXTRACT(HOUR FROM so.created_at) <= par.hour_4h_ago
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
    GROUP BY so.warehouse_id, pso.product_id
),

-- Step 12: Calculate Achievement Percentage for 4 Hours Ago and Compare with Current
achievement_trend AS (
    SELECT 
        ut.product_id,
        ut.warehouse_id,
        -- Current achievement percentage
        CASE 
            WHEN ut.uth_target > 0 
            THEN (COALESCE(au.actual_qty_uth, 0) / ut.uth_target) * 100
            ELSE NULL
        END AS current_achievement_pct,
        -- Achievement percentage 4 hours ago
        CASE 
            WHEN ut4.uth_target_4h_ago > 0 
            THEN (COALESCE(au4.actual_qty_uth_4h_ago, 0) / ut4.uth_target_4h_ago) * 100
            ELSE NULL
        END AS achievement_pct_4h_ago
    FROM uth_targets ut
    LEFT JOIN actual_uth au 
        ON au.product_id = ut.product_id 
        AND au.warehouse_id = ut.warehouse_id
    LEFT JOIN uth_targets_4h_ago ut4
        ON ut4.product_id = ut.product_id 
        AND ut4.warehouse_id = ut.warehouse_id
    LEFT JOIN actual_uth_4h_ago au4
        ON au4.product_id = ut.product_id 
        AND au4.warehouse_id = ut.warehouse_id
)

-- Final Output
SELECT 
    bd.product_id,
	bd.warehouse_id,
	bd.sku,
	bd.brand,
	bd.cat,
    ut.uth_target,
    --ut.uth_contribution,
    COALESCE(au.actual_qty_uth, 0) AS actual_qty_uth,
    -- Achievement percentage
    CASE 
        WHEN ut.uth_target > 0 
        THEN (COALESCE(au.actual_qty_uth, 0) / ut.uth_target) * 100
        ELSE NULL
    END AS achievement_percentage,
    -- Current price (use from Pricing_data_extraction if available, else from cohort prices)
    -- Note: If Pricing_data_extraction already has current_price column, exclude it from bd.* above to avoid duplicate
    cp.current_price AS current_price,
    -- Effective price (after both discounts)
    ep.effective_price,
    -- Discount flags
    COALESCE(skd.has_sku_discount, 0) AS has_sku_discount,
    COALESCE(qdd.has_qty_discount, 0) AS has_qty_discount,
    -- Stock value
    bd.available_stock * COALESCE(bd.wac1, 0) AS stock_value,
    -- Achievement trend (increasing or decreasing)
    CASE 
        WHEN at.current_achievement_pct IS NULL OR at.achievement_pct_4h_ago IS NULL THEN NULL
        WHEN at.current_achievement_pct > at.achievement_pct_4h_ago THEN 'increasing'
        WHEN at.current_achievement_pct < at.achievement_pct_4h_ago THEN 'decreasing'
        ELSE 'stable'
    END AS achievement_trend
FROM base_data bd
LEFT JOIN uth_targets ut 
    ON ut.product_id = bd.product_id 
    AND ut.warehouse_id = bd.warehouse_id
LEFT JOIN actual_uth au 
    ON au.product_id = bd.product_id 
    AND au.warehouse_id = bd.warehouse_id
LEFT JOIN current_prices cp 
    ON cp.product_id = bd.product_id
    AND cp.warehouse_id = bd.warehouse_id
LEFT JOIN effective_prices ep 
    ON ep.product_id = bd.product_id 
    AND ep.warehouse_id = bd.warehouse_id
LEFT JOIN active_sku_discounts skd 
    ON skd.product_id = bd.product_id 
    AND skd.warehouse_id = bd.warehouse_id
LEFT JOIN active_qd_discounts qdd 
    ON qdd.product_id = bd.product_id 
    AND qdd.warehouse_id = bd.warehouse_id
LEFT JOIN achievement_trend at
    ON at.product_id = bd.product_id 
    AND at.warehouse_id = bd.warehouse_id
ORDER BY stock_value desc

