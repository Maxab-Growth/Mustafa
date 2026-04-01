WITH latest_per_sku AS (
    SELECT product_name_ar,
           MAX(created_at::date) AS max_date
    FROM materialized_views.raw_scraped_data
    WHERE created_at::date >=  CURRENT_TIMESTAMP::date - 4
    GROUP BY product_name_ar
)
select source_app,supplier,r.product_name_ar as scrapped_product,brand as scrapped_brand,quantity_per_unit,unit_type,price as scrapped_price
from materialized_views.RAW_SCRAPED_DATA r
JOIN latest_per_sku l
      ON r.product_name_ar = l.product_name_ar
     AND r.created_at::date = l.max_date
group by all
