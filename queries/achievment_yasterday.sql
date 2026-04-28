WITH parent_whs AS (
    SELECT * FROM (VALUES (236, 343), (1, 467), (962, 343)) x(parent_id, child_id)
),
warehouse_mapping as (
select * 
from (
values 
    ('Cairo', 'Mostorod', 1, 700),
    ('Giza', 'Barageel', 236, 701),
    ('Giza', 'Sakkarah', 962, 701),
    ('Delta West', 'El-Mahala', 337, 703),
    ('Delta West', 'Tanta', 8, 703),
    ('Delta East', 'Mansoura FC', 339, 704),
    ('Delta East', 'Sharqya', 170, 704),
    ('Upper Egypt', 'Assiut FC', 501, 1124),
    ('Upper Egypt', 'Bani sweif', 401, 1126),
    ('Upper Egypt', 'Menya Samalot', 703, 1123),
    ('Upper Egypt', 'Sohag', 632, 1125),
    ('Alexandria', 'Khorshed Alex', 797, 702)
)x(region,warehouse_name,warehouse_id,cohort_id)
),

yest_sales AS (
    SELECT
        COALESCE(pw.parent_id, pso.warehouse_id) AS warehouse_id,
        pso.product_id,
        SUM(pso.purchased_item_count*basic_unit_count)			AS yest_qty,
        SUM(pso.total_price)                     				AS yest_nmv,
        COUNT(DISTINCT so.retailer_id)           				AS yest_retailers
    FROM product_sales_order pso
    LEFT JOIN parent_whs pw ON pw.child_id = pso.warehouse_id
    JOIN sales_orders so    ON so.id = pso.sales_order_id
    WHERE so.created_at::DATE = CURRENT_DATE - 1
      AND so.sales_order_status_id NOT IN (7, 12)
      AND so.channel IN ('telesales', 'retailer')
      AND pso.purchased_item_count <> 0
    GROUP BY 1, 2
),

benchmarks AS (
    SELECT product_id, warehouse_id, cohort_id,
           p80_daily_240d, p70_daily_retailers_240d,
           current_price, wac_p, cat,
           ROUND((current_price - wac_p) / NULLIF(current_price, 0), 4) AS margin
    FROM MATERIALIZED_VIEWS.Pricing_data_extraction
    WHERE created_at = (SELECT MAX(created_at) FROM MATERIALIZED_VIEWS.Pricing_data_extraction)
),

closing_stocks AS (
    SELECT product_id, warehouse_id,
           CASE WHEN closing_child IS NOT NULL AND closing_stocks = 0
                THEN closing_child ELSE closing_stocks END AS stocks
    FROM (
        SELECT sdc.product_id, sdc.warehouse_id,
               sdc.available_stock  AS closing_stocks,
               sdc2.available_stock AS closing_child
        FROM  materialized_views.stock_day_close sdc
        LEFT JOIN parent_whs pw ON pw.parent_id = sdc.warehouse_id
        LEFT JOIN materialized_views.stock_day_close sdc2
               ON sdc2.warehouse_id    = pw.child_id
              AND sdc2.product_id      = sdc.product_id
              AND sdc2.packing_unit_id = sdc.packing_unit_id
              AND sdc2.timestamp       = sdc.timestamp
        WHERE sdc.timestamp::DATE = CURRENT_DATE - 1
          AND sdc.warehouse_id NOT IN (6, 9, 10)
    )
    WHERE stocks > 0
),

opening_stocks AS (
    SELECT product_id, warehouse_id,
           CASE WHEN opening_child IS NOT NULL AND opening_stocks = 0
                THEN opening_child ELSE opening_stocks END AS stocks
    FROM (
        SELECT sdc.product_id, sdc.warehouse_id,
               sdc.available_stock  AS opening_stocks,
               sdc2.available_stock AS opening_child
        FROM  materialized_views.stock_day_close sdc
        LEFT JOIN parent_whs pw ON pw.parent_id = sdc.warehouse_id
        LEFT JOIN materialized_views.stock_day_close sdc2
               ON sdc2.warehouse_id    = pw.child_id
              AND sdc2.product_id      = sdc.product_id
              AND sdc2.packing_unit_id = sdc.packing_unit_id
              AND sdc2.timestamp       = sdc.timestamp
        WHERE sdc.timestamp::DATE = CURRENT_DATE - 2
          AND sdc.warehouse_id NOT IN (6, 9, 10)
    )
    WHERE stocks > 0
),
comm_cons as (
WITH to_remove AS (
    SELECT 
        check_date AS start_date,
        (check_date + INTERVAL '1 month') + 6 AS end_date 
    FROM (
        SELECT 
            CASE 
                WHEN DATE_PART('day', Current_timestamp::DATE) < 7 
                THEN DATE_TRUNC('month', Current_timestamp::DATE - INTERVAL '1 month') 
                ELSE DATE_FROM_PARTS(
                    YEAR(Current_timestamp::DATE), 
                    MONTH(Current_timestamp::DATE), 
                    1
                )  
            END AS check_date
    )
),
region_mapping AS (
    SELECT * FROM (VALUES
        ('Greater Cairo', 'Cairo'),
        ('Greater Cairo', 'Giza')
    ) x(region, main_region)
)
SELECT  distinct 
    sku_id AS product_id,
    coalesce(rm.main_region, comm.region) AS region,
	cohort_id,
    min_price AS commercial_min_price
FROM (
    SELECT 
        mp.product_id AS sku_id,
        mp.region,
        min_price,
		wac1,
        created_at,
        MAX(created_at) OVER (PARTITION BY mp.product_id, mp.region) AS latest_date
    FROM finance.minimum_prices mp
	join finance.all_cogs f on f.product_id = mp.product_id and CURRENT_TIMESTAMP between f.from_date and f.to_date
    WHERE is_deleted = 'false'
        AND created_at BETWEEN (SELECT start_date FROM to_remove) AND (SELECT end_date FROM to_remove)
) comm
LEFT JOIN region_mapping rm ON comm.region = rm.region
left join warehouse_mapping wm on wm.region = coalesce(rm.main_region, comm.region)
WHERE created_at = latest_date
and commercial_min_price < wac1*1.5
)

SELECT distinct 
    b.warehouse_id,
    b.cohort_id,
    b.product_id,
    CONCAT(p.name_ar, ' ', p.size, ' ', pu.name_ar) AS sku,
    br.name_ar                                      AS brand,
    b.cat,
    COALESCE(y.yest_qty, 0)                         AS yest_qty,
    COALESCE(y.yest_retailers, 0)                   AS yest_retailers,
    ROUND(b.p80_daily_240d, 1)                      AS p80_target,
    ROUND(b.p70_daily_retailers_240d, 1)            AS p70_ret_target,
    ROUND(COALESCE(y.yest_qty, 0)       / NULLIF(b.p80_daily_240d, 0), 2)           AS qty_ratio,
    ROUND(COALESCE(y.yest_retailers, 0) / NULLIF(b.p70_daily_retailers_240d, 0), 2) AS ret_ratio,
    ROUND(b.current_price, 2)                       AS current_price,
    ROUND(b.wac_p, 2)                               AS wac_p,
    ROUND(b.margin, 2)                              AS margin_pct,
    os.stocks                                       AS opening_stock,
    s.stocks                                        AS closing_stock,
    ROUND(b.current_price * s.stocks, 2)            AS stock_value,
	coalesce(commercial_min_price,0)				AS min_price
FROM benchmarks b
JOIN closing_stocks s  ON s.product_id  = b.product_id AND s.warehouse_id  = b.warehouse_id
JOIN opening_stocks os ON os.product_id = b.product_id AND os.warehouse_id = b.warehouse_id
LEFT JOIN yest_sales y ON y.product_id  = b.product_id AND y.warehouse_id  = b.warehouse_id
JOIN products      p   ON p.id          = b.product_id
JOIN brands        br  ON br.id         = p.brand_id
JOIN product_units pu  ON pu.id         = p.unit_id
left join comm_cons ccn on ccn.product_id = b.product_id and ccn.cohort_id = b.cohort_id
WHERE b.p80_daily_240d > 0
 --AND COALESCE(y.yest_qty, 0)       / NULLIF(b.p80_daily_240d, 0)           < 0.8
  --AND COALESCE(y.yest_retailers, 0) / NULLIF(b.p70_daily_retailers_240d, 0) < 0.70
  --AND os.stocks >= b.p80_daily_240d * 0.5
ORDER BY stock_value DESC;