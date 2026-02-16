-- =============================================================================
-- YESTERDAY ACTIONS ASSESSMENT QUERY
-- =============================================================================
-- This query assesses yesterday's pricing actions at SKU-warehouse level
-- Shows performance metrics, discounts, and filters to top 10 stock contributors per bucket
--
-- Key Metrics:
-- - Buckets from yesterday's initial push (or calculated from yesterday's qty vs P80)
-- - Quantity benchmark = P80 qty (p80_daily_240d)
-- - Achievement percentage = (yesterday_qty / qty_benchmark) * 100
-- - Average margin and price yesterday vs day before
-- - SKU and quantity discount percentages
-- - Discount flags (active at any time during yesterday)
-- - Top 10 stock contributors per bucket
-- =============================================================================

WITH 
-- Step 1: Get Yesterday's Initial Push Data
yesterday_initial_push AS (
    SELECT 
        product_id,
        warehouse_id,
        cohort_id,
        yesterday_status,
        stocks AS yesterday_stocks,
        abc_class,
        sku,
        brand,
        cat
    FROM MATERIALIZED_VIEWS.pricing_initial_push
    WHERE created_at::date = CURRENT_DATE - 1
),

-- Step 2: Get P80, WAC, and Market Price Percentiles from Pricing_data_extraction
pricing_data AS (
    SELECT DISTINCT
        product_id,
        warehouse_id,
        p80_daily_240d,
        wac_p,
        wac1,
        minimum,
        percentile_25,
        percentile_50,
        percentile_75,
        maximum
    FROM MATERIALIZED_VIEWS.Pricing_data_extraction
    WHERE created_at::date = CURRENT_DATE - 1
),

-- Step 3: Get Yesterday's Sales Data
yesterday_sales AS (
    SELECT 
        so.warehouse_id,
        pso.product_id,
        SUM(pso.purchased_item_count) AS yesterday_qty,
        SUM(pso.total_price) AS yesterday_nmv,
        -- Average price per basic unit: total_price / (purchased_item_count * basic_unit_count)
        SUM(pso.total_price) / NULLIF(SUM(pso.purchased_item_count * pso.basic_unit_count), 0) AS yesterday_avg_price,
        -- Average margin: (price - wac_p) / price
        -- Calculate margin per order line, then average
        AVG(
            CASE 
                WHEN pso.total_price > 0 AND pso.purchased_item_count > 0 AND pso.basic_unit_count > 0 AND f.wac_p > 0
                THEN (pso.total_price / (pso.purchased_item_count * pso.basic_unit_count) - f.wac_p) 
                     / NULLIF(pso.total_price / (pso.purchased_item_count * pso.basic_unit_count), 0)
                ELSE NULL
            END
        ) AS yesterday_avg_margin,
        -- SKU discount percentage: (discount_value / item_price) * 100
        -- Average across all order lines
        AVG(
            CASE 
                WHEN pso.total_price > 0 AND pso.purchased_item_count > 0 AND pso.basic_unit_count > 0 
                     AND pso.ITEM_DISCOUNT_VALUE > 0
                THEN (pso.ITEM_DISCOUNT_VALUE / NULLIF(pso.total_price / (pso.purchased_item_count * pso.basic_unit_count), 0)) * 100
                ELSE NULL
            END
        ) AS yesterday_sku_disc_pct,
        -- Quantity discount percentage: (discount_value / item_price) * 100
        -- Average across all order lines
        AVG(
            CASE 
                WHEN pso.total_price > 0 AND pso.purchased_item_count > 0 AND pso.basic_unit_count > 0 
                     AND pso.ITEM_QUANTITY_DISCOUNT_VALUE > 0
                THEN (pso.ITEM_QUANTITY_DISCOUNT_VALUE / NULLIF(pso.total_price / (pso.purchased_item_count * pso.basic_unit_count), 0)) * 100
                ELSE NULL
            END
        ) AS yesterday_qty_disc_pct
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    JOIN finance.all_cogs f ON f.product_id = pso.product_id
        AND so.created_at::date BETWEEN f.from_date::date AND f.to_date::date
    WHERE so.created_at::date = CURRENT_DATE - 1
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
    GROUP BY so.warehouse_id, pso.product_id
),

-- Step 4: Get Day Before Sales Data (for comparison)
day_before_sales AS (
    SELECT 
        so.warehouse_id,
        pso.product_id,
        -- Average price per basic unit
        SUM(pso.total_price) / NULLIF(SUM(pso.purchased_item_count * pso.basic_unit_count), 0) AS day_before_avg_price,
        -- Average margin: (price - wac_p) / price
        AVG(
            CASE 
                WHEN pso.total_price > 0 AND pso.purchased_item_count > 0 AND pso.basic_unit_count > 0 AND f.wac_p > 0
                THEN (pso.total_price / (pso.purchased_item_count * pso.basic_unit_count) - f.wac_p) 
                     / NULLIF(pso.total_price / (pso.purchased_item_count * pso.basic_unit_count), 0)
                ELSE NULL
            END
        ) AS day_before_avg_margin
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    JOIN finance.all_cogs f ON f.product_id = pso.product_id
        AND so.created_at::date BETWEEN f.from_date::date AND f.to_date::date
    WHERE so.created_at::date = CURRENT_DATE - 2
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
    GROUP BY so.warehouse_id, pso.product_id
),

-- Step 5: Get SKU Discount Flags (active at ANY time during yesterday)
-- SKU discounts are linked to retailers, which are mapped to warehouses via polygons
sku_discount_flags AS (
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
)f
        WHERE 
             sd.name_en = 'Special Discounts'
            -- Active at any time during yesterday
            AND sd.start_at <= (CURRENT_DATE - 1)::timestamp + INTERVAL '1 day' - INTERVAL '1 second'
            AND sd.end_at >= (CURRENT_DATE - 1)::timestamp
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
        1 AS had_active_sku_disc
    FROM sku_discount_warehouse_mapping
),

-- Step 6: Get Quantity Discount Flags (active at ANY time during yesterday)
-- Quantity discounts are linked to products via quantity_discount_values
-- and to warehouses via dynamic_tags
quantity_discount_flags AS (
    WITH qd_with_products AS (
        SELECT DISTINCT
            qd.id AS qd_id,
            qdv.product_id,
            qd.dynamic_tag_id,
            qd.start_at,
            qd.end_at
        FROM quantity_discounts qd
        JOIN quantity_discount_values qdv ON qdv.quantity_discount_id = qd.id
        WHERE 
            -- Active at any time during yesterday
             qd.start_at <= (CURRENT_DATE - 1)::timestamp + INTERVAL '1 day' - INTERVAL '1 second'
            AND qd.end_at >= (CURRENT_DATE - 1)::timestamp
    ),
    qd_warehouse_mapping AS (
        SELECT DISTINCT
            qwp.product_id,
            wdr.warehouse_id
        FROM qd_with_products qwp
        JOIN dynamic_tags dt ON dt.id = qwp.dynamic_tag_id
        JOIN WAREHOUSE_DISPATCHING_RULES wdr ON wdr.product_id = qwp.product_id
        JOIN DISPATCHING_POLYGONS dp ON dp.id = wdr.DISPATCHING_POLYGON_ID
        JOIN materialized_views.retailer_polygon rp ON rp.district_id = dp.district_id
        WHERE dt.name LIKE '%QD_rets%'
    )
    SELECT DISTINCT
        product_id,
        warehouse_id,
        1 AS had_active_qty_disc
    FROM qd_warehouse_mapping
),

-- Step 7: Combine all data and calculate buckets
combined_data AS (
    SELECT 
        yip.product_id,
        yip.warehouse_id,
        yip.cohort_id,
        yip.sku AS sku_name,
        yip.brand,
        yip.cat,
        yip.yesterday_status,
        yip.yesterday_stocks,
        yip.abc_class,
        pd.p80_daily_240d AS qty_benchmark,
        pd.wac_p,
        pd.wac1,
        pd.minimum,
        pd.percentile_25,
        pd.percentile_50,
        pd.percentile_75,
        pd.maximum,
        COALESCE(ys.yesterday_qty, 0) AS yesterday_qty,
        ys.yesterday_avg_price,
        ys.yesterday_avg_margin,
        ys.yesterday_sku_disc_pct,
        ys.yesterday_qty_disc_pct,
        dbs.day_before_avg_price,
        dbs.day_before_avg_margin,
        COALESCE(sdf.had_active_sku_disc, 0) AS had_active_sku_disc,
        COALESCE(qdf.had_active_qty_disc, 0) AS had_active_qty_disc,
        -- Calculate bucket based on yesterday's qty vs P80 (direct comparison, no std)
        -- Buckets are from yesterday's classification logic
        CASE 
            WHEN yip.yesterday_stocks <= 0 THEN 'OOS'
            WHEN COALESCE(ys.yesterday_qty, 0) = 0 AND yip.yesterday_stocks > 0 THEN 'Zero Demand'
            WHEN pd.p80_daily_240d > 0 AND COALESCE(ys.yesterday_qty, 0) > pd.p80_daily_240d THEN 'Above Target'
            WHEN pd.p80_daily_240d > 0 AND COALESCE(ys.yesterday_qty, 0) < pd.p80_daily_240d THEN 'Below Target'
            WHEN pd.p80_daily_240d > 0 AND COALESCE(ys.yesterday_qty, 0) = pd.p80_daily_240d THEN 'On Track'
            ELSE 'No Data'
        END AS bucket
    FROM yesterday_initial_push yip
    LEFT JOIN pricing_data pd ON pd.product_id = yip.product_id 
        AND pd.warehouse_id = yip.warehouse_id
    LEFT JOIN yesterday_sales ys ON ys.product_id = yip.product_id 
        AND ys.warehouse_id = yip.warehouse_id
    LEFT JOIN day_before_sales dbs ON dbs.product_id = yip.product_id 
        AND dbs.warehouse_id = yip.warehouse_id
    LEFT JOIN sku_discount_flags sdf ON sdf.product_id = yip.product_id 
        AND sdf.warehouse_id = yip.warehouse_id
    LEFT JOIN quantity_discount_flags qdf ON qdf.product_id = yip.product_id 
        AND qdf.warehouse_id = yip.warehouse_id
),

-- Step 8: Calculate additional metrics
metrics_calculated AS (
    SELECT 
        *,
        -- Stock contribution (stock value)
        yesterday_stocks * COALESCE(wac_p, 0) AS stock_contribution,
        -- Achievement percentage
        CASE 
            WHEN qty_benchmark > 0 
            THEN (yesterday_qty / qty_benchmark) * 100 
            ELSE NULL 
        END AS achievement_pct,
        -- Price change percentage
        CASE 
            WHEN day_before_avg_price > 0 AND yesterday_avg_price > 0
            THEN ((yesterday_avg_price - day_before_avg_price) / day_before_avg_price) * 100
            ELSE NULL
        END AS price_change_pct,
        -- Margin change percentage
        CASE 
            WHEN day_before_avg_margin IS NOT NULL AND yesterday_avg_margin IS NOT NULL
            THEN ((yesterday_avg_margin - day_before_avg_margin) / ABS(day_before_avg_margin)) * 100
            ELSE NULL
        END AS margin_change_pct,
        -- Price after discount = avg_price - SKU discount - QD discount
        CASE 
            WHEN yesterday_avg_price > 0
            THEN yesterday_avg_price 
                 - COALESCE(yesterday_avg_price * (yesterday_sku_disc_pct / 100), 0)
                 - COALESCE(yesterday_avg_price * (yesterday_qty_disc_pct / 100), 0)
            ELSE NULL
        END AS yesterday_price_after_discount,
        -- Market position for yesterday's average price
        CASE 
            WHEN minimum IS NULL THEN NULL
            WHEN yesterday_avg_price < minimum * 0.98 THEN 'below_market'
            WHEN yesterday_avg_price <= minimum * 1.02 THEN 'at_market_min'
            WHEN percentile_25 IS NOT NULL AND yesterday_avg_price >= percentile_25 * 0.98 AND yesterday_avg_price <= percentile_25 * 1.02 THEN 'at_market_25pct'
            WHEN percentile_50 IS NOT NULL AND yesterday_avg_price >= percentile_50 * 0.98 AND yesterday_avg_price <= percentile_50 * 1.02 THEN 'at_market_median'
            WHEN percentile_75 IS NOT NULL AND yesterday_avg_price >= percentile_75 * 0.98 AND yesterday_avg_price <= percentile_75 * 1.02 THEN 'at_market_75pct'
            WHEN maximum IS NOT NULL AND yesterday_avg_price >= maximum * 0.98 AND yesterday_avg_price <= maximum * 1.02 THEN 'at_market_max'
            WHEN maximum IS NOT NULL AND yesterday_avg_price > maximum * 1.02 THEN 'above_market'
            ELSE NULL
        END AS yesterday_avg_price_market_position,
        -- Market position for yesterday's price after discount
        CASE 
            WHEN minimum IS NULL THEN NULL
            WHEN yesterday_price_after_discount IS NULL THEN NULL
            WHEN yesterday_price_after_discount < minimum * 0.98 THEN 'below_market'
            WHEN yesterday_price_after_discount <= minimum * 1.02 THEN 'at_market_min'
            WHEN percentile_25 IS NOT NULL AND yesterday_price_after_discount >= percentile_25 * 0.98 AND yesterday_price_after_discount <= percentile_25 * 1.02 THEN 'at_market_25pct'
            WHEN percentile_50 IS NOT NULL AND yesterday_price_after_discount >= percentile_50 * 0.98 AND yesterday_price_after_discount <= percentile_50 * 1.02 THEN 'at_market_median'
            WHEN percentile_75 IS NOT NULL AND yesterday_price_after_discount >= percentile_75 * 0.98 AND yesterday_price_after_discount <= percentile_75 * 1.02 THEN 'at_market_75pct'
            WHEN maximum IS NOT NULL AND yesterday_price_after_discount >= maximum * 0.98 AND yesterday_price_after_discount <= maximum * 1.02 THEN 'at_market_max'
            WHEN maximum IS NOT NULL AND yesterday_price_after_discount > maximum * 1.02 THEN 'above_market'
            ELSE NULL
        END AS yesterday_price_after_discount_market_position
    FROM combined_data
),

-- Step 9: Get Module 2 Actions (Initial Push)
module2_actions AS (
    SELECT 
        product_id,
        warehouse_id,
        current_price AS module2_current_price,
        new_price AS module2_new_price,
        price_change AS module2_price_change,
        price_change_pct AS module2_price_change_pct,
        price_action AS module2_price_action,
        price_reason AS module2_price_reason,
        price_source AS module2_price_source,
        current_cart_rule AS module2_current_cart_rule,
        new_cart_rule AS module2_new_cart_rule
    FROM MATERIALIZED_VIEWS.pricing_initial_push
    WHERE created_at::date = CURRENT_DATE - 1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, warehouse_id ORDER BY created_at DESC) = 1
),

-- Step 10: Get Module 3 Actions (Periodic)
module3_actions AS (
    SELECT 
        product_id,
        warehouse_id,
        current_price AS module3_current_price,
        new_price AS module3_new_price,
        price_action AS module3_price_action,
        action_reason AS module3_action_reason,
        current_cart_rule AS module3_current_cart_rule,
        new_cart_rule AS module3_new_cart_rule,
        activate_sku_discount AS module3_activate_sku_discount,
        activate_qd AS module3_activate_qd,
        uth_status AS module3_uth_status
    FROM MATERIALIZED_VIEWS.pricing_periodic_push
    WHERE created_at::date = CURRENT_DATE - 1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, warehouse_id ORDER BY created_at DESC) = 1
),

-- Step 11: Get Module 4 Actions (Hourly)
module4_actions AS (
    SELECT 
        product_id,
        warehouse_id,
        current_price AS module4_current_price,
        new_price AS module4_new_price,
        price_action AS module4_price_action,
        cart_rule_action AS module4_cart_rule_action,
        current_cart_rule AS module4_current_cart_rule,
        new_cart_rule AS module4_new_cart_rule,
        uth_qty_status AS module4_uth_qty_status,
        last_hour_qty_status AS module4_last_hour_qty_status
    FROM MATERIALIZED_VIEWS.pricing_hourly_push
    WHERE created_at::date = CURRENT_DATE - 1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, warehouse_id ORDER BY created_at DESC) = 1
),

-- Step 12: Determine Final Action Taken (prioritize Module 4 > 3 > 2)
final_actions AS (
    SELECT 
        product_id,
        warehouse_id,
        -- Price action (Module 4 takes precedence)
        COALESCE(m4.module4_price_action, m3.module3_price_action, m2.module2_price_action) AS final_price_action,
        -- Price changed flag
        CASE 
            WHEN (m4.module4_new_price IS NOT NULL AND m4.module4_new_price <> m4.module4_current_price)
                 OR (m3.module3_new_price IS NOT NULL AND m3.module3_new_price <> m3.module3_current_price)
                 OR (m2.module2_new_price IS NOT NULL AND m2.module2_new_price <> m2.module2_current_price)
            THEN 1 
            ELSE 0 
        END AS final_price_changed,
        -- Price change percentage (from final module)
        COALESCE(
            CASE WHEN m4.module4_new_price IS NOT NULL AND m4.module4_current_price > 0 
                 THEN ((m4.module4_new_price - m4.module4_current_price) / m4.module4_current_price) * 100 
                 ELSE NULL END,
            CASE WHEN m3.module3_new_price IS NOT NULL AND m3.module3_current_price > 0 
                 THEN ((m3.module3_new_price - m3.module3_current_price) / m3.module3_current_price) * 100 
                 ELSE NULL END,
            m2.module2_price_change_pct
        ) AS final_price_change_pct,
        -- Cart rule changed flag
        CASE 
            WHEN (m4.module4_new_cart_rule IS NOT NULL AND m4.module4_new_cart_rule <> m4.module4_current_cart_rule)
                 OR (m3.module3_new_cart_rule IS NOT NULL AND m3.module3_new_cart_rule <> m3.module3_current_cart_rule)
                 OR (m2.module2_new_cart_rule IS NOT NULL AND m2.module2_new_cart_rule <> m2.module2_current_cart_rule)
            THEN 1 
            ELSE 0 
        END AS final_cart_rule_changed,
        -- Action reason (from final module)
        COALESCE(m4.module4_cart_rule_action, m3.module3_action_reason, m2.module2_price_reason) AS final_action_reason
    FROM (
        SELECT DISTINCT product_id, warehouse_id 
        FROM (
            SELECT product_id, warehouse_id FROM module4_actions
            UNION
            SELECT product_id, warehouse_id FROM module3_actions
            UNION
            SELECT product_id, warehouse_id FROM module2_actions
        )
    ) all_actions
    LEFT JOIN module4_actions m4 
        ON all_actions.product_id = m4.product_id AND all_actions.warehouse_id = m4.warehouse_id
    LEFT JOIN module3_actions m3 
        ON all_actions.product_id = m3.product_id AND all_actions.warehouse_id = m3.warehouse_id
    LEFT JOIN module2_actions m2 
        ON all_actions.product_id = m2.product_id AND all_actions.warehouse_id = m2.warehouse_id
),

-- Step 13: Join actions to metrics
metrics_with_actions AS (
    SELECT 
        mc.*,
        m2.module2_price_action,
        m2.module2_price_change_pct,
        m2.module2_price_reason,
        m3.module3_price_action,
        m3.module3_action_reason,
        m3.module3_activate_sku_discount,
        m3.module3_activate_qd,
        m4.module4_price_action,
        m4.module4_cart_rule_action,
        fa.final_price_action,
        fa.final_price_changed,
        fa.final_price_change_pct,
        fa.final_cart_rule_changed,
        fa.final_action_reason
    FROM metrics_calculated mc
    LEFT JOIN module2_actions m2 ON m2.product_id = mc.product_id AND m2.warehouse_id = mc.warehouse_id
    LEFT JOIN module3_actions m3 ON m3.product_id = mc.product_id AND m3.warehouse_id = mc.warehouse_id
    LEFT JOIN module4_actions m4 ON m4.product_id = mc.product_id AND m4.warehouse_id = mc.warehouse_id
    LEFT JOIN final_actions fa ON fa.product_id = mc.product_id AND fa.warehouse_id = mc.warehouse_id
),

-- Step 14: Rank and filter to top 10 per bucket
ranked_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY yesterday_status ORDER BY stock_contribution DESC) AS rank_in_bucket
    FROM metrics_with_actions
    WHERE stock_contribution > 0  -- Only include SKUs with stock value
)

-- Step 10: Final output - Top 10 per bucket
SELECT 
    product_id,
    warehouse_id,
    cohort_id,
    sku_name,
    brand,
    cat,
    bucket,
    yesterday_status,
    yesterday_stocks,
    stock_contribution,
    qty_benchmark,
    yesterday_qty,
    achievement_pct,
    yesterday_avg_price,
    day_before_avg_price,
    price_change_pct,
    yesterday_avg_margin,
    day_before_avg_margin,
    margin_change_pct,
    COALESCE(yesterday_sku_disc_pct, 0) AS yesterday_sku_disc_pct,
    COALESCE(yesterday_qty_disc_pct, 0) AS yesterday_qty_disc_pct,
    had_active_sku_disc,
    had_active_qty_disc,
    yesterday_avg_price_market_position,
    yesterday_price_after_discount_market_position,
    module2_price_action,
    module2_price_change_pct,
    module2_price_reason,
    module3_price_action,
    module3_action_reason,
    module3_activate_sku_discount,
    module3_activate_qd,
    module4_price_action,
    module4_cart_rule_action,
    final_price_action,
    final_price_changed,
    final_price_change_pct,
    final_cart_rule_changed,
    final_action_reason,
    rank_in_bucket
FROM ranked_data
WHERE rank_in_bucket <= 15
ORDER BY 
    bucket,
    rank_in_bucket;

