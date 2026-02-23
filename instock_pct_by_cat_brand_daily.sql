-- =============================================================================
-- Instock percentage per category & brand, daily
-- Weighted by SKU contribution per warehouse (NMV-based)
-- Output: date, brand, cat, instock_percentage
-- =============================================================================

WITH params AS (
    SELECT
        CONVERT_TIMEZONE('Africa/Cairo', CURRENT_TIMESTAMP())::DATE AS today,
        CONVERT_TIMEZONE('Africa/Cairo', CURRENT_TIMESTAMP())::DATE - 1    AS yesterday,
        CONVERT_TIMEZONE('Africa/Cairo', CURRENT_TIMESTAMP())::DATE - 180 AS contribution_start  -- 6M for stable weights
),

-- Product lookup: product_id -> brand, cat
product_lookup AS (
    SELECT
        p.id AS product_id,
        b.name_ar AS brand,
        c.name_ar AS cat
    FROM products p
    JOIN brands b ON b.id = p.brand_id
    JOIN categories c ON c.id = p.category_id
),

-- Historical NMV per (warehouse, product) for contribution (last 6 months)
warehouse_product_nmv AS (
    SELECT
        pso.warehouse_id,
        pso.product_id,
        pl.cat,
        pl.brand,
        SUM(pso.total_price) AS nmv
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    JOIN product_lookup pl ON pl.product_id = pso.product_id
    CROSS JOIN params pr
    WHERE so.created_at::DATE >= pr.contribution_start
      AND so.created_at::DATE < pr.today
      AND so.sales_order_status_id NOT IN (7, 12)
      AND so.channel IN ('telesales', 'retailer')
      AND pso.purchased_item_count <> 0
      AND pl.cat IS NOT NULL
      AND pl.brand IS NOT NULL
    GROUP BY pso.warehouse_id, pso.product_id, pl.cat, pl.brand
),

-- SKU contribution per warehouse (within each warehouse, cat, brand)
sku_contribution_wh AS (
    SELECT
        warehouse_id,
        product_id,
        cat,
        brand,
        nmv / NULLIF(SUM(nmv) OVER (PARTITION BY warehouse_id, cat, brand), 0) AS sku_cntrb_wh
    FROM warehouse_product_nmv
),

-- Daily stock: in-stock flag per (warehouse, product, date)
-- In-stock = opening > 0 AND closing > 0
daily_stock AS (
    SELECT
        sdc.warehouse_id,
        sdc.product_id,
        sdc.TIMESTAMP::DATE AS stock_date,
        CASE
            WHEN LAG(sdc.available_stock, 1) OVER (
                     PARTITION BY sdc.warehouse_id, sdc.product_id
                     ORDER BY sdc.TIMESTAMP::DATE
                 ) > 0
                 AND sdc.available_stock > 0
            THEN 1
            ELSE 0
        END AS in_stock_flag
    FROM materialized_views.stock_day_close sdc
    CROSS JOIN params pr
    WHERE sdc.TIMESTAMP::DATE >= pr.contribution_start
      AND sdc.TIMESTAMP::DATE < pr.today
),
daily_stock_dedup AS (
    SELECT
        warehouse_id,
        product_id,
        stock_date,
        MAX(in_stock_flag) AS in_stock_flag
    FROM daily_stock
    GROUP BY 1, 2, 3
),

-- Join contribution with daily stock (only for SKUs that have contribution)
daily_wh_cat_brand_weighted AS (
    SELECT
        d.stock_date,
        sc.warehouse_id,
        sc.cat,
        sc.brand,
        SUM(sc.sku_cntrb_wh * COALESCE(d.in_stock_flag, 0)) AS weighted_instock_pct
    FROM sku_contribution_wh sc
    LEFT JOIN daily_stock_dedup d
        ON d.warehouse_id = sc.warehouse_id
       AND d.product_id = sc.product_id
    GROUP BY d.stock_date, sc.warehouse_id, sc.cat, sc.brand
    HAVING d.stock_date IS NOT NULL
),

-- Daily instock percentage per (date, cat, brand): average across warehouses
daily_cat_brand_instock AS (
    SELECT
        stock_date AS date,
        cat,
        brand,
        AVG(weighted_instock_pct) AS instock_percentage
    FROM daily_wh_cat_brand_weighted
    GROUP BY stock_date, cat, brand
)

SELECT
    date,
    brand,
    cat,
    instock_percentage
FROM daily_cat_brand_instock
ORDER BY date DESC, brand, cat;
