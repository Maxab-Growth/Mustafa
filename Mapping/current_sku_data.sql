with current_prices as (
select 
        pu.product_id,
		PACKING_UNIT_ID,
		BASIC_UNIT_COUNT,
        AVG(cpu.price) AS current_price
    FROM cohort_product_packing_units cpu
    JOIN PACKING_UNIT_PRODUCTS pu ON pu.id = cpu.product_packing_unit_id
    WHERE cpu.cohort_id IN (700)
        AND cpu.created_at::date <> '2023-07-31'
        AND cpu.is_customized = TRUE
    GROUP BY ALL
),
current_stocks as (
with parent_whs as (
select * 
from (
VALUES
(236,343),
(1,467),
(962,343)
)x(parent_id,child_id)

)
select product_id,sum(stocks) as stocks 
from(
select warehouse_id,product_id,case when stocks_child is not null and stocks = 0 then stocks_child else stocks end as stocks 
from (
SELECT 
    pw.warehouse_id,
    pw.product_id,
    pw.available_stock::INTEGER AS stocks,
	pw2.available_stock::INTEGER AS stocks_child
	

FROM product_warehouse pw
left join parent_whs p on p.parent_id = pw.warehouse_id
left join product_warehouse pw2 on pw2.warehouse_id = p.child_id and pw.product_id = pw2.product_id
WHERE pw.warehouse_id IN (1, 8, 170, 236, 337, 339, 401, 501, 632, 703, 797, 962,343,467)
    AND pw.is_basic_unit = 1
)
)
group by all
),
current_sales as (
WITH parent_whs AS (
    SELECT * FROM (VALUES (236,343),(1,467),(962,343)) x(parent_id,child_id)
),
nmv_last_4m AS (
    SELECT 
        COALESCE(pw.parent_id, pso.warehouse_id) AS warehouse_id,
        pso.product_id,
        SUM(pso.total_price) AS total_nmv_4m
    FROM product_sales_order pso
    LEFT JOIN parent_whs pw ON pw.child_id = pso.warehouse_id
    JOIN sales_orders so ON so.id = pso.sales_order_id
    WHERE so.created_at::DATE >=  CURRENT_TIMESTAMP::DATE - 120
        AND so.created_at::DATE <  CURRENT_TIMESTAMP::DATE
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
    GROUP BY COALESCE(pw.parent_id, pso.warehouse_id), pso.product_id
    HAVING SUM(pso.total_price) > 0
)
SELECT 
    product_id,
    sum(total_nmv_4m) as nmv
FROM nmv_last_4m
group by all
)
select distinct p.id as product_id ,
CONCAT(p.name_ar,' ',p.size,' ',product_units.name_ar) as sku,b.name_ar as brand,c.name_ar as cat,current_price,PACKING_UNIT_ID,BASIC_UNIT_COUNT,
pu.name_ar as packing_unit_name_ar ,pu.name_en as packing_unit_name_en,stocks,nmv

from products p 
join categories c on c.id = p.category_id
join brands b on b.id = p.brand_id
JOIN product_units ON product_units.id = p.unit_id 
join current_prices cp on cp.product_id = p.id 
join PACKING_UNITS pu on pu.id = cp.PACKING_UNIT_ID
join current_stocks cs on p.id = cs.product_id
join current_sales csa on csa.product_id = p.id
