-- =============================================================================
-- UTH LIVE QUERY: Today's Up-Till-Hour Performance with Discount Contributions
-- =============================================================================
-- Run in Module 3 to get today's actual performance vs targets
-- Includes SKU discount and QD tier contributions calculated from today's sales
-- Parameters: 
--   {timezone} - Timezone for conversion (e.g., 'UTC')
-- =============================================================================

WITH params AS (
    SELECT
        CONVERT_TIMEZONE('{timezone}', 'Africa/Cairo', CURRENT_TIMESTAMP())::DATE AS today,
        HOUR(CONVERT_TIMEZONE('{timezone}', 'Africa/Cairo', CURRENT_TIMESTAMP())) AS current_hour
),

-- Map dynamic tags to warehouse IDs using name matching
qd_det AS (
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

-- Get current active QD configurations
qd_config AS (
    SELECT * 
    FROM (
        SELECT 
            product_id,
            start_at,
            end_at,
            packing_unit_id,
            id AS qd_id,
            qd.warehouse_id,
            MAX(CASE WHEN tier = 1 THEN quantity END) AS tier_1_qty,
            MAX(CASE WHEN tier = 1 THEN discount_percentage END) AS tier_1_discount_pct,
            MAX(CASE WHEN tier = 2 THEN quantity END) AS tier_2_qty,
            MAX(CASE WHEN tier = 2 THEN discount_percentage END) AS tier_2_discount_pct,
            MAX(CASE WHEN tier = 3 THEN quantity END) AS tier_3_qty,
            MAX(CASE WHEN tier = 3 THEN discount_percentage END) AS tier_3_discount_pct
        FROM (
            SELECT 
                qd.id,
                qdv.product_id,
                qdv.packing_unit_id,
                qdv.quantity,
                qdv.discount_percentage,
                qd.dynamic_tag_id,
                qd.start_at,
                qd.end_at,
                ROW_NUMBER() OVER (
                    PARTITION BY qdv.product_id, qdv.packing_unit_id, qd.id 
                    ORDER BY qdv.quantity
                ) AS tier
            FROM quantity_discounts qd 
            JOIN quantity_discount_values qdv ON qdv.quantity_discount_id = qd.id
            WHERE qd.deleted_at IS NULL 
                AND qd.is_active = TRUE
                AND qd.start_at::DATE <= CONVERT_TIMEZONE('{timezone}', 'Africa/Cairo', CURRENT_TIMESTAMP())::DATE
        ) qd_tiers
        JOIN qd_det qd ON qd.tag_id = qd_tiers.dynamic_tag_id
        GROUP BY ALL
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, packing_unit_id, warehouse_id ORDER BY start_at DESC) = 1
),

-- Today's sales up-till-hour with discount breakdown
today_uth_sales AS (
    SELECT 
        pso.warehouse_id,
        pso.product_id,
        so.retailer_id,
        pso.packing_unit_id,
        pso.purchased_item_count AS qty,
        pso.total_price AS nmv,
        pso.ITEM_DISCOUNT_VALUE AS sku_discount_per_unit,
        pso.ITEM_QUANTITY_DISCOUNT_VALUE AS qty_discount_per_unit,
        qd.tier_1_qty,
        qd.tier_2_qty,
        qd.tier_3_qty,
        -- Determine tier used based on quantity purchased vs tier thresholds
        CASE 
            WHEN pso.ITEM_QUANTITY_DISCOUNT_VALUE = 0 OR qd.tier_1_qty IS NULL THEN 'Base'
            WHEN qd.tier_3_qty IS NOT NULL AND pso.purchased_item_count >= qd.tier_3_qty THEN 'Tier 3'
            WHEN qd.tier_2_qty IS NOT NULL AND pso.purchased_item_count >= qd.tier_2_qty THEN 'Tier 2'
            WHEN qd.tier_1_qty IS NOT NULL AND pso.purchased_item_count >= qd.tier_1_qty THEN 'Tier 1'
            ELSE 'Base'
        END AS tier_used
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    LEFT JOIN qd_config qd 
        ON qd.product_id = pso.product_id 
        AND qd.packing_unit_id = pso.packing_unit_id
        AND qd.warehouse_id = so.warehouse_id
    CROSS JOIN params p
    WHERE so.created_at::DATE = p.today
        AND HOUR(so.created_at) < p.current_hour
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
)

SELECT 
    warehouse_id,
    product_id,
    -- Basic UTH metrics
    SUM(qty) AS uth_qty,
    SUM(nmv) AS uth_nmv,
    COUNT(DISTINCT retailer_id) AS uth_retailers,
    
    -- SKU discount NMV and contribution (% of total NMV from SKU discounted sales)
    SUM(CASE WHEN sku_discount_per_unit > 0 THEN nmv ELSE 0 END) AS sku_discount_nmv_uth,
    ROUND(SUM(CASE WHEN sku_discount_per_unit > 0 THEN nmv ELSE 0 END) * 100.0 / NULLIF(SUM(nmv), 0), 2) AS sku_disc_cntrb_uth,
    
    -- Quantity discount NMV and contribution (% of total NMV from QD sales)
    SUM(CASE WHEN qty_discount_per_unit > 0 THEN nmv ELSE 0 END) AS qty_discount_nmv_uth,
    ROUND(SUM(CASE WHEN qty_discount_per_unit > 0 THEN nmv ELSE 0 END) * 100.0 / NULLIF(SUM(nmv), 0), 2) AS qty_disc_cntrb_uth,
    
    -- Tier-level NMV
    SUM(CASE WHEN tier_used = 'Tier 1' THEN nmv ELSE 0 END) AS t1_nmv_uth,
    SUM(CASE WHEN tier_used = 'Tier 2' THEN nmv ELSE 0 END) AS t2_nmv_uth,
    SUM(CASE WHEN tier_used = 'Tier 3' THEN nmv ELSE 0 END) AS t3_nmv_uth,
    
    -- Tier-level contributions (% of total NMV from each tier)
    ROUND(SUM(CASE WHEN tier_used = 'Tier 1' THEN nmv ELSE 0 END) * 100.0 / NULLIF(SUM(nmv), 0), 2) AS t1_cntrb_uth,
    ROUND(SUM(CASE WHEN tier_used = 'Tier 2' THEN nmv ELSE 0 END) * 100.0 / NULLIF(SUM(nmv), 0), 2) AS t2_cntrb_uth,
    ROUND(SUM(CASE WHEN tier_used = 'Tier 3' THEN nmv ELSE 0 END) * 100.0 / NULLIF(SUM(nmv), 0), 2) AS t3_cntrb_uth,
    
    -- Query metadata
    (SELECT current_hour FROM params) AS query_hour
FROM today_uth_sales
GROUP BY warehouse_id, product_id
HAVING SUM(nmv) > 0
