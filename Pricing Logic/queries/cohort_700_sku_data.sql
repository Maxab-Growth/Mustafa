-- =============================================================================
-- COHORT 700 SKU DATA QUERY
-- =============================================================================
-- This query retrieves SKU information for cohort 700, including:
-- - product_id, product_name
-- - packing_unit_id, packing_unit_name
-- - basic_unit_count
-- - current_price
-- - product_unit
-- 
-- Only includes SKUs that have sales in the last 120 days
-- =============================================================================

WITH 
-- Step 1: Get SKUs with sales in the last 120 days
skus_with_sales AS (
    SELECT DISTINCT
        pso.product_id
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    WHERE so.created_at >= CURRENT_DATE - INTERVAL '120 days'
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
),

-- Step 2: Get current prices from cohort_product_packing_units for cohort 700
current_prices AS (
    SELECT 
        pu.product_id,
        pu.packing_unit_id,
        AVG(cpu.price) AS current_price
    FROM cohort_product_packing_units cpu
    JOIN PACKING_UNIT_PRODUCTS pu ON pu.id = cpu.product_packing_unit_id
    WHERE cpu.cohort_id = 700
        AND cpu.created_at::date <> '2023-07-31'
        AND cpu.is_customized = TRUE
    GROUP BY pu.product_id, pu.packing_unit_id
),

-- Step 3: Get products fetched by bensoliman in the last 5 days
bensoliman_products AS (
    SELECT DISTINCT
        sm.maxab_product_id AS product_id
    FROM materialized_views.savvy_mapping sm
    WHERE sm.bs_price IS NOT NULL
        AND sm.INJECTION_DATE::date >= CURRENT_DATE - 5
)

-- Final Output
SELECT DISTINCT
    p.id AS product_id,
    CONCAT(p.name_ar,' ',p.size,' ',prod_units.name_ar) AS product_name,
    p.size AS size,
    prod_units.name_ar AS product_unit,
    pu.packing_unit_id,
    pack_units.name_ar AS packing_unit_name,
    pu.basic_unit_count,
    cp.current_price,
    CASE WHEN bs.product_id IS NOT NULL THEN 1 ELSE 0 END AS ben_soliman,
    cat.name_ar AS cat,
    b.name_ar AS brand
FROM skus_with_sales sws
JOIN products p ON p.id = sws.product_id
JOIN PACKING_UNIT_PRODUCTS pu ON pu.product_id = sws.product_id 
JOIN packing_units pack_units ON pack_units.id = pu.packing_unit_id
JOIN product_units prod_units ON prod_units.id = p.unit_id
JOIN brands b ON b.id = p.brand_id
JOIN categories cat ON cat.id = p.category_id
JOIN current_prices cp ON cp.product_id = sws.product_id 
    AND cp.packing_unit_id = pu.packing_unit_id
LEFT JOIN bensoliman_products bs ON bs.product_id = p.id
ORDER BY p.id, pu.packing_unit_id

