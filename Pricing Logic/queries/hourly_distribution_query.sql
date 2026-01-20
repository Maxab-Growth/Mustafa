-- =============================================================================
-- HOURLY DISTRIBUTION QUERY: Historical UTH Patterns (Last 4 Months)
-- =============================================================================
-- Calculates what % of daily qty/retailers are typically sold by each hour
-- Used to set UTH targets in Module 3
-- Parameters:
--   {current_hour} - Current hour to calculate cumulative % up to
--   {timezone} - Timezone for conversion
--   {warehouse_ids} - Comma-separated warehouse IDs
-- =============================================================================

WITH params AS (
    SELECT
        CONVERT_TIMEZONE('{timezone}', 'Africa/Cairo', CURRENT_TIMESTAMP())::DATE AS today,
        CONVERT_TIMEZONE('{timezone}', 'Africa/Cairo', CURRENT_TIMESTAMP())::DATE - 120 AS history_start,
        {current_hour} AS target_hour
),

-- Hourly sales aggregation (last 4 months)
hourly_sales AS (
    SELECT
        pso.warehouse_id,
        pso.product_id,
        so.created_at::DATE AS sale_date,
        EXTRACT(HOUR FROM so.created_at) AS sale_hour,
        SUM(pso.purchased_item_count * pso.basic_unit_count) AS hourly_qty,
        COUNT(DISTINCT so.retailer_id) AS hourly_retailers
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    CROSS JOIN params p
    WHERE so.created_at::DATE >= p.history_start
        AND so.created_at::DATE < p.today
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
        AND pso.warehouse_id IN ({warehouse_ids})
    GROUP BY pso.warehouse_id, pso.product_id, so.created_at::DATE, EXTRACT(HOUR FROM so.created_at)
),

-- Daily totals
daily_totals AS (
    SELECT
        warehouse_id,
        product_id,
        sale_date,
        SUM(hourly_qty) AS daily_qty,
        SUM(hourly_retailers) AS daily_retailers
    FROM hourly_sales
    GROUP BY warehouse_id, product_id, sale_date
),

-- Cumulative by hour for each day
cumulative_by_hour AS (
    SELECT
        h.warehouse_id,
        h.product_id,
        h.sale_date,
        h.sale_hour,
        SUM(h.hourly_qty) OVER (
            PARTITION BY h.warehouse_id, h.product_id, h.sale_date 
            ORDER BY h.sale_hour 
            ROWS UNBOUNDED PRECEDING
        ) AS cumulative_qty,
        SUM(h.hourly_retailers) OVER (
            PARTITION BY h.warehouse_id, h.product_id, h.sale_date 
            ORDER BY h.sale_hour 
            ROWS UNBOUNDED PRECEDING
        ) AS cumulative_retailers,
        d.daily_qty,
        d.daily_retailers
    FROM hourly_sales h
    JOIN daily_totals d ON h.warehouse_id = d.warehouse_id 
        AND h.product_id = d.product_id 
        AND h.sale_date = d.sale_date
),

-- Average cumulative % at target hour
avg_hourly_pct AS (
    SELECT
        warehouse_id,
        product_id,
        AVG(CASE WHEN daily_qty > 0 THEN cumulative_qty / daily_qty ELSE 0 END) AS hourly_qty_pct,
        AVG(CASE WHEN daily_retailers > 0 THEN cumulative_retailers / daily_retailers ELSE 0 END) AS hourly_retailer_pct,
        COUNT(DISTINCT sale_date) AS days_with_data
    FROM cumulative_by_hour
    CROSS JOIN params p
    WHERE sale_hour = p.target_hour
    GROUP BY warehouse_id, product_id
)

SELECT
    warehouse_id,
    product_id,
    ROUND(hourly_qty_pct, 4) AS hourly_qty_pct,
    ROUND(hourly_retailer_pct, 4) AS hourly_retailer_pct,
    days_with_data,
    (SELECT target_hour FROM params) AS target_hour
FROM avg_hourly_pct
WHERE days_with_data >= 10  -- Need at least 10 days of data

