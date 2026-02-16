-- =============================================================================
-- DAILY ACTION EFFECTIVENESS ANALYSIS QUERY
-- =============================================================================
-- This query analyzes yesterday's pricing actions and their effectiveness by:
-- - Getting base data from Module 2 (yesterday_status, current_price, stocks)
-- - Calculating sold qty and effective discounts from sales
-- - Getting SKU discounts that started yesterday (average discount)
-- - Getting quantity discount tiers (T1, T2, T3) with quantities and discounts
-- - Calculating effective discounts per tier from actual sales
-- - Getting pricing actions from cohort_pricing_changes (mapped to warehouse)
-- - Calculating start_price and closing_price for the day
-- - Calculating achievement percentage (yesterday vs day before)
--
-- Purpose: Understand how effective yesterday's actions were by comparing
-- achievement percentage versus the day before achievement
-- =============================================================================

WITH 
-- Step 1: Get Module 2 Base Data (yesterday_status, current_price, stocks, wac_p, target_qty)
module2_base AS (
    SELECT 
        m2.product_id,
        m2.warehouse_id,
        m2.yesterday_status,
        m2.current_price,
        m2.stocks,
        m2.sku,
        m2.brand,
        m2.cat,
        pd.wac_p,
        pd.p80_daily_240d AS target_qty
    FROM MATERIALIZED_VIEWS.pricing_initial_push m2
    LEFT JOIN MATERIALIZED_VIEWS.Pricing_data_extraction pd
        ON pd.product_id = m2.product_id
        AND pd.warehouse_id = m2.warehouse_id
        AND pd.created_at::date = CURRENT_DATE - 1
    WHERE m2.created_at::date = CURRENT_DATE - 1
),

-- Step 2: Get Yesterday's Sales Data (sold_qty, effective discount calculations)
yesterday_sales AS (
    SELECT 
        so.warehouse_id,
        pso.product_id,
        SUM(pso.purchased_item_count) AS sold_qty,
        -- Effective discount percentage from actual sales
        -- Calculate as: (ITEM_DISCOUNT_VALUE / unit_price) * 100
        AVG(
            CASE 
                WHEN pso.total_price > 0 
                     AND pso.purchased_item_count > 0 
                     AND pso.basic_unit_count > 0
                     AND pso.ITEM_DISCOUNT_VALUE > 0
                THEN (pso.ITEM_DISCOUNT_VALUE / NULLIF(
                    pso.total_price / (pso.purchased_item_count * pso.basic_unit_count), 
                    0
                )) * 100
                ELSE NULL
            END
        ) AS effective_perc
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    WHERE so.created_at::date = CURRENT_DATE - 1
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
    GROUP BY so.warehouse_id, pso.product_id
),

-- Step 3: Get Day Before Sales Data (for achievement comparison)
day_before_sales AS (
    SELECT 
        so.warehouse_id,
        pso.product_id,
        SUM(pso.purchased_item_count) AS day_before_sold_qty
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    WHERE so.created_at::date = CURRENT_DATE - 2
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
    GROUP BY so.warehouse_id, pso.product_id
),

-- Step 4: Get SKU Discounts Starting Yesterday (average discount mapped to warehouse)
sku_discounts_yesterday AS (
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
            AND sd.start_at::date = CURRENT_DATE - 1  -- Discounts starting yesterday
    ),
    sku_discount_warehouse_mapping AS (
        SELECT DISTINCT
            sdv.product_id,
            sdv.discount_percentage,
            wdr.warehouse_id,
            asd.retailer_id
        FROM active_sku_discount asd
        JOIN SKU_DISCOUNT_VALUES sdv ON sdv.sku_discount_id = asd.sku_discount_id
        JOIN materialized_views.retailer_polygon rp ON rp.retailer_id = asd.retailer_id
        JOIN WAREHOUSE_DISPATCHING_RULES wdr ON wdr.product_id = sdv.product_id
        JOIN DISPATCHING_POLYGONS dp ON dp.id = wdr.DISPATCHING_POLYGON_ID 
            AND dp.district_id = rp.district_id
    ),
    sku_discount_by_tier AS (
        SELECT 
            product_id,
            warehouse_id,
            discount_percentage,
            COUNT(DISTINCT retailer_id) AS retailer_count
        FROM sku_discount_warehouse_mapping
        GROUP BY product_id, warehouse_id, discount_percentage
    )
    SELECT 
        product_id,
        warehouse_id,
        -- Weighted average discount: SUM(discount * retailer_count) / SUM(retailer_count)
        SUM(discount_percentage * retailer_count) / NULLIF(SUM(retailer_count), 0) AS sku_discount_perc
    FROM sku_discount_by_tier
    GROUP BY product_id, warehouse_id
),

-- Step 5: Get Quantity Discount Tiers (T1, T2, T3 quantities and discounts)
quantity_discount_tiers AS (
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
    ),
    qd_config AS (
        SELECT 
            product_id,
            packing_unit_id,
            qd.warehouse_id,
            qd_tiers.start_at AS discount_start_at,
            MAX(CASE WHEN tier = 1 THEN quantity END) AS qd_tier_1_qty,
            MAX(CASE WHEN tier = 1 THEN discount_percentage END) AS qd_tier_1_disc_pct,
            MAX(CASE WHEN tier = 2 THEN quantity END) AS qd_tier_2_qty,
            MAX(CASE WHEN tier = 2 THEN discount_percentage END) AS qd_tier_2_disc_pct,
            MAX(CASE WHEN tier = 3 THEN quantity END) AS qd_tier_3_qty,
            MAX(CASE WHEN tier = 3 THEN discount_percentage END) AS qd_tier_3_disc_pct
        FROM (
            SELECT 
                qd.id,
                qdv.product_id,
                qdv.packing_unit_id,
                qdv.quantity,
                qdv.discount_percentage,
                qd.dynamic_tag_id,
                qd.start_at,
                ROW_NUMBER() OVER (
                    PARTITION BY qdv.product_id, qdv.packing_unit_id, qd.id 
                    ORDER BY qdv.quantity
                ) AS tier
            FROM quantity_discounts qd 
            JOIN quantity_discount_values qdv ON qdv.quantity_discount_id = qd.id
            WHERE qd.start_at::date = CURRENT_DATE - 1
        ) qd_tiers
        JOIN qd_det qd ON qd.tag_id = qd_tiers.dynamic_tag_id
        GROUP BY product_id, packing_unit_id, qd.warehouse_id, qd_tiers.start_at
    )
    -- Convert to basic unit level (filter to basic_unit_count = 1) and aggregate by product-warehouse
    -- If multiple QDs were active during the day, get minimum quantity and minimum discount per tier
    SELECT 
        qc.product_id,
        qc.warehouse_id,
        MIN(qc.qd_tier_1_qty) AS t1_qty,
        MIN(qc.qd_tier_1_disc_pct) AS t1_disc,
        MIN(qc.qd_tier_2_qty) AS t2_qty,
        MIN(qc.qd_tier_2_disc_pct) AS t2_disc,
        MIN(qc.qd_tier_3_qty) AS t3_qty,
        MIN(qc.qd_tier_3_disc_pct) AS t3_disc,
        MIN(qc.discount_start_at) AS discount_start_at
    FROM qd_config qc
    JOIN PACKING_UNIT_PRODUCTS pup ON pup.product_id = qc.product_id 
        AND pup.packing_unit_id = qc.packing_unit_id
    WHERE pup.basic_unit_count = 1
    GROUP BY 
        qc.product_id,
        qc.warehouse_id
),

-- Step 6: Get QD Effective Discounts Per Tier from Sales Data
qd_effective_by_tier AS (
    WITH sales_with_tiers AS (
        SELECT 
            qt.warehouse_id,
            qt.product_id,
            pso.purchased_item_count AS qty,
            pso.total_price,
            pso.ITEM_QUANTITY_DISCOUNT_VALUE,
            pso.basic_unit_count,
            qt.t1_qty,
            qt.t2_qty,
            qt.t3_qty,
            qt.discount_start_at
        FROM quantity_discount_tiers qt
        JOIN product_sales_order pso ON pso.product_id = qt.product_id
        JOIN sales_orders so ON so.id = pso.sales_order_id
            AND so.warehouse_id = qt.warehouse_id
        WHERE so.created_at::date = CURRENT_DATE - 1  -- Only yesterday's sales
            AND so.sales_order_status_id NOT IN (7, 12)
            AND so.channel IN ('telesales', 'retailer')
            AND pso.purchased_item_count <> 0
    )
    SELECT 
        warehouse_id,
        product_id,
        -- T1 effective discount: (discount_value / unit_price) * 100 for T1 orders
        AVG(
            CASE 
                WHEN t1_qty IS NOT NULL 
                     AND qty >= t1_qty 
                     AND (t2_qty IS NULL OR qty < t2_qty)
                     AND ITEM_QUANTITY_DISCOUNT_VALUE > 0
                THEN (ITEM_QUANTITY_DISCOUNT_VALUE / NULLIF(
                    total_price / (qty * basic_unit_count), 
                    0
                )) * 100
                ELSE NULL
            END
        ) AS t1_effective_disc,
        -- T2 effective discount
        AVG(
            CASE 
                WHEN t2_qty IS NOT NULL 
                     AND qty >= t2_qty 
                     AND (t3_qty IS NULL OR qty < t3_qty)
                     AND ITEM_QUANTITY_DISCOUNT_VALUE > 0
                THEN (ITEM_QUANTITY_DISCOUNT_VALUE / NULLIF(
                    total_price / (qty * basic_unit_count), 
                    0
                )) * 100
                ELSE NULL
            END
        ) AS t2_effective_disc,
        -- T3 effective discount
        AVG(
            CASE 
                WHEN t3_qty IS NOT NULL 
                     AND qty >= t3_qty
                     AND ITEM_QUANTITY_DISCOUNT_VALUE > 0
                THEN (ITEM_QUANTITY_DISCOUNT_VALUE / NULLIF(
                    total_price / (qty * basic_unit_count), 
                    0
                )) * 100
                ELSE NULL
            END
        ) AS t3_effective_disc
    FROM sales_with_tiers
    GROUP BY warehouse_id, product_id
),

-- Step 7: Get Pricing Actions from cohort_pricing_changes (mapped to warehouse)
warehouse_cohort_mapping AS (
    SELECT * FROM VALUES
        (700, 1),    -- Cairo -> Mostorod
        (701, 236),  -- Giza -> Barageel
        (701, 962),  -- Giza -> Sakkarah
        (702, 797),  -- Alexandria -> Khorshed Alex
        (703, 337),  -- Delta West -> El-Mahala
        (703, 8),    -- Delta West -> Tanta
        (704, 339),  -- Delta East -> Mansoura FC
        (704, 170),  -- Delta East -> Sharqya
        (1123, 703), -- Upper Egypt -> Menya Samalot
        (1124, 501), -- Upper Egypt -> Assiut FC
        (1125, 632), -- Upper Egypt -> Sohag
        (1126, 401)  -- Upper Egypt -> Bani sweif
    AS x(cohort_id, warehouse_id)
),
pricing_actions AS (
    SELECT 
        wcm.warehouse_id,
        pu.product_id,
        cpc.price,
        cpc.created_at
    FROM cohort_pricing_changes cpc
    JOIN PACKING_UNIT_PRODUCTS pu ON pu.id = cpc.product_packing_unit_id
    JOIN warehouse_cohort_mapping wcm ON wcm.cohort_id = cpc.cohort_id
    WHERE cpc.created_at::date = CURRENT_DATE - 1
        AND pu.is_basic_unit = 1
        AND cpc.cohort_id IN (700, 701, 702, 703, 704, 1123, 1124, 1125, 1126)
),

-- Step 8: Get Start Price and Closing Price from Pricing Changes
start_closing_prices AS (
    WITH price_changes_ordered AS (
        SELECT 
            warehouse_id,
            product_id,
            price,
            created_at,
            ROW_NUMBER() OVER (
                PARTITION BY warehouse_id, product_id 
                ORDER BY created_at ASC
            ) AS first_price_rn,
            ROW_NUMBER() OVER (
                PARTITION BY warehouse_id, product_id 
                ORDER BY created_at DESC
            ) AS last_price_rn
        FROM pricing_actions
    )
    SELECT 
        warehouse_id,
        product_id,
        -- Start price: first price of the day (earliest price change)
        MAX(CASE WHEN first_price_rn = 1 THEN price END) AS start_price,
        -- Closing price: last price of the day (latest price change)
        MAX(CASE WHEN last_price_rn = 1 THEN price END) AS closing_price
    FROM price_changes_ordered
    GROUP BY warehouse_id, product_id
),

-- Step 9: Calculate Achievement Percentage (yesterday and day before)
achievement_calc AS (
    SELECT 
        m2.product_id,
        m2.warehouse_id,
        m2.target_qty,
        COALESCE(ys.sold_qty, 0) AS sold_qty,
        COALESCE(dbs.day_before_sold_qty, 0) AS day_before_sold_qty,
        -- Achievement percentage: (sold_qty / target_qty) * 100
        CASE 
            WHEN m2.target_qty > 0 
            THEN (COALESCE(ys.sold_qty, 0) / m2.target_qty) * 100 
            ELSE NULL 
        END AS achievement_percentage,
        -- Day before achievement percentage
        CASE 
            WHEN m2.target_qty > 0 
            THEN (COALESCE(dbs.day_before_sold_qty, 0) / m2.target_qty) * 100 
            ELSE NULL 
        END AS day_before_achievement_percentage
    FROM module2_base m2
    LEFT JOIN yesterday_sales ys 
        ON ys.product_id = m2.product_id 
        AND ys.warehouse_id = m2.warehouse_id
    LEFT JOIN day_before_sales dbs 
        ON dbs.product_id = m2.product_id 
        AND dbs.warehouse_id = m2.warehouse_id
)

-- Step 10: Final Output - Combine All Data
SELECT 
    m2.product_id,
    m2.warehouse_id,
    m2.sku,
    m2.brand,
    m2.cat,
    m2.stocks,
    m2.stocks * COALESCE(m2.wac_p, 0) AS stock_value,
    m2.yesterday_status,
    ac.target_qty,
    ac.sold_qty,
    COALESCE(scp.start_price, m2.current_price) AS start_price,
    COALESCE(scp.closing_price, m2.current_price) AS closing_price,
    COALESCE(skd.sku_discount_perc, 0) AS sku_discount_perc,
    COALESCE(ys.effective_perc, 0) AS effective_perc,
    qdt.t1_qty,
    COALESCE(qdt.t1_disc, 0) AS t1_disc,
    COALESCE(qet.t1_effective_disc, 0) AS t1_effective_disc,
    qdt.t2_qty,
    COALESCE(qdt.t2_disc, 0) AS t2_disc,
    COALESCE(qet.t2_effective_disc, 0) AS t2_effective_disc,
    qdt.t3_qty,
    COALESCE(qdt.t3_disc, 0) AS t3_disc,
    COALESCE(qet.t3_effective_disc, 0) AS t3_effective_disc,
    ac.achievement_percentage,
    ac.day_before_achievement_percentage,
    -- Calculate achievement change
    ac.achievement_percentage - ac.day_before_achievement_percentage AS achievement_change_pct
FROM module2_base m2
LEFT JOIN achievement_calc ac 
    ON ac.product_id = m2.product_id 
    AND ac.warehouse_id = m2.warehouse_id
LEFT JOIN start_closing_prices scp 
    ON scp.product_id = m2.product_id 
    AND scp.warehouse_id = m2.warehouse_id
LEFT JOIN yesterday_sales ys 
    ON ys.product_id = m2.product_id 
    AND ys.warehouse_id = m2.warehouse_id
LEFT JOIN sku_discounts_yesterday skd 
    ON skd.product_id = m2.product_id 
    AND skd.warehouse_id = m2.warehouse_id
LEFT JOIN quantity_discount_tiers qdt 
    ON qdt.product_id = m2.product_id 
    AND qdt.warehouse_id = m2.warehouse_id
LEFT JOIN qd_effective_by_tier qet 
    ON qet.product_id = m2.product_id 
    AND qet.warehouse_id = m2.warehouse_id
where m2.product_id = 130 	
ORDER BY 
    m2.stocks DESC,
    ac.achievement_percentage DESC NULLS LAST;

