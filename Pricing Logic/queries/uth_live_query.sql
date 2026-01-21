-- =============================================================================
-- UTH LIVE QUERY: Today's Up-Till-Hour Performance
-- =============================================================================
-- Run in Module 3 to get today's actual performance vs targets
-- Parameters: 
--   {current_hour} - Current hour (0-23)
--   {timezone} - Timezone for conversion
--   {warehouse_ids} - Comma-separated warehouse IDs
-- =============================================================================

WITH params AS (
    SELECT
        CONVERT_TIMEZONE('{timezone}', 'Africa/Cairo', CURRENT_TIMESTAMP())::DATE AS today,
        EXTRACT(HOUR FROM CONVERT_TIMEZONE('{timezone}', 'Africa/Cairo', CURRENT_TIMESTAMP())) AS current_hour
),

-- Today's sales up-till-hour (UTH)
today_uth AS (
    SELECT
        pso.warehouse_id,
        pso.product_id,
        SUM(pso.purchased_item_count * pso.basic_unit_count) AS qty_uth,
        SUM(pso.net_value) AS nmv_uth,
        COUNT(DISTINCT so.retailer_id) AS retailers_uth
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    CROSS JOIN params p
    WHERE so.created_at::DATE = p.today
        AND EXTRACT(HOUR FROM so.created_at) <= p.current_hour
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
        AND pso.warehouse_id IN ({warehouse_ids})
    GROUP BY pso.warehouse_id, pso.product_id
),

-- Today's discount contributions (UTH)
today_discount_cntrb AS (
    SELECT
        pso.warehouse_id,
        pso.product_id,
        -- SKU discount contribution
        SUM(CASE WHEN pso.promo_discount > 0 THEN pso.net_value ELSE 0 END) * 100.0 / 
            NULLIF(SUM(pso.net_value), 0) AS sku_disc_cntrb_uth,
        -- Quantity discount contribution (total)
        SUM(CASE WHEN pso.qd_discount > 0 THEN pso.net_value ELSE 0 END) * 100.0 / 
            NULLIF(SUM(pso.net_value), 0) AS qty_disc_cntrb_uth,
        -- Tier-level contributions (approximate based on tier thresholds)
        SUM(CASE WHEN pso.qd_tier = 1 THEN pso.net_value ELSE 0 END) * 100.0 / 
            NULLIF(SUM(pso.net_value), 0) AS t1_cntrb_uth,
        SUM(CASE WHEN pso.qd_tier = 2 THEN pso.net_value ELSE 0 END) * 100.0 / 
            NULLIF(SUM(pso.net_value), 0) AS t2_cntrb_uth,
        SUM(CASE WHEN pso.qd_tier = 3 THEN pso.net_value ELSE 0 END) * 100.0 / 
            NULLIF(SUM(pso.net_value), 0) AS t3_cntrb_uth
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    CROSS JOIN params p
    WHERE so.created_at::DATE = p.today
        AND EXTRACT(HOUR FROM so.created_at) <= p.current_hour
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.warehouse_id IN ({warehouse_ids})
    GROUP BY pso.warehouse_id, pso.product_id
)

SELECT
    u.warehouse_id,
    u.product_id,
    u.qty_uth,
    u.nmv_uth,
    u.retailers_uth,
    COALESCE(d.sku_disc_cntrb_uth, 0) AS sku_disc_cntrb_uth,
    COALESCE(d.qty_disc_cntrb_uth, 0) AS qty_disc_cntrb_uth,
    COALESCE(d.t1_cntrb_uth, 0) AS t1_cntrb_uth,
    COALESCE(d.t2_cntrb_uth, 0) AS t2_cntrb_uth,
    COALESCE(d.t3_cntrb_uth, 0) AS t3_cntrb_uth,
    (SELECT current_hour FROM params) AS query_hour
FROM today_uth u
LEFT JOIN today_discount_cntrb d 
    ON u.warehouse_id = d.warehouse_id AND u.product_id = d.product_id

