-- =============================================================================
-- SKU WAREHOUSE DAILY REPORT
-- =============================================================================
-- Per SKU-warehouse report showing:
--   - Qty sold day before yesterday and yesterday
--   - Price reduction / increase percentage (net, first-to-last price yesterday)
--   - Average active SKU discount percentage (blended weighted by retailer count)
--   - Average active quantity discount percentage (mean of configured tier %s)
--   - Average selling price vs market median
--   - Margins before and after discounts from actual sales
--
-- Grain: product_id + warehouse_id
-- =============================================================================

WITH
-- Product metadata from yesterday's data extraction
product_info AS (
    SELECT
        product_id,
        warehouse_id,
        sku,
        brand,
        cat,
        stocks,
        wac_p,
        percentile_50 AS market_median,
        price_position
    FROM MATERIALIZED_VIEWS.Pricing_data_extraction
    WHERE created_at::date = CURRENT_DATE - 1
),

-- Actual sold qty (basic units) for the day before yesterday
sold_qty_day_before AS (
    SELECT
        so.warehouse_id,
        pso.product_id,
        SUM(pso.purchased_item_count * pso.basic_unit_count) AS sold_qty
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    WHERE so.created_at::date = CURRENT_DATE - 2
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
    GROUP BY so.warehouse_id, pso.product_id
),

-- Actual sold qty (basic units) and average selling price for yesterday
sold_qty_yesterday AS (
    SELECT
        so.warehouse_id,
        pso.product_id,
        SUM(pso.purchased_item_count * pso.basic_unit_count) AS sold_qty,
        SUM(pso.total_price) / NULLIF(SUM(pso.purchased_item_count * pso.basic_unit_count), 0) AS avg_selling_price,
        SUM(pso.total_price
            - COALESCE(pso.ITEM_DISCOUNT_VALUE, 0) * pso.purchased_item_count * pso.basic_unit_count
            - COALESCE(pso.ITEM_QUANTITY_DISCOUNT_VALUE, 0) * pso.purchased_item_count * pso.basic_unit_count
        ) / NULLIF(SUM(pso.purchased_item_count * pso.basic_unit_count), 0) AS avg_selling_price_after_discount
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    WHERE so.created_at::date = CURRENT_DATE - 1
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
    GROUP BY so.warehouse_id, pso.product_id
),

-- Cohort-to-warehouse mapping for price changes
warehouse_cohort_mapping AS (
    SELECT * FROM VALUES
        (700, 1),
        (701, 236),
        (701, 962),
        (702, 797),
        (703, 337),
        (703, 8),
        (704, 339),
        (704, 170),
        (1123, 703),
        (1124, 501),
        (1125, 632),
        (1126, 401)
    AS x(cohort_id, warehouse_id)
),

-- Yesterday's price changes from cohort_pricing_changes (basic unit only)
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

-- First (start) and last (closing) price of the day per SKU-warehouse
start_closing_prices AS (
    WITH price_changes_ordered AS (
        SELECT
            warehouse_id,
            product_id,
            price,
            ROW_NUMBER() OVER (
                PARTITION BY warehouse_id, product_id ORDER BY created_at ASC
            ) AS first_rn,
            ROW_NUMBER() OVER (
                PARTITION BY warehouse_id, product_id ORDER BY created_at DESC
            ) AS last_rn
        FROM pricing_actions
    )
    SELECT
        warehouse_id,
        product_id,
        MAX(CASE WHEN first_rn = 1 THEN price END) AS start_price,
        MAX(CASE WHEN last_rn  = 1 THEN price END) AS closing_price
    FROM price_changes_ordered
    GROUP BY warehouse_id, product_id
),

-- SKU discounts active yesterday (blended weighted average mapped to warehouse)
sku_discounts_yesterday AS (
    WITH active_sku_discount AS (
        SELECT
            sd.id AS sku_discount_id,
            f.value::INT AS retailer_id
        FROM SKU_DISCOUNTS sd,
        LATERAL FLATTEN(
            input => SPLIT(
                REPLACE(REPLACE(REPLACE(sd.retailer_ids, '{', ''), '}', ''), '"', ''),
                ','
            )
        ) f
        WHERE sd.name_en = 'Special Discounts'
            AND CURRENT_DATE - 1 BETWEEN sd.start_at::date AND sd.end_at::date
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
        SUM(discount_percentage * retailer_count) / NULLIF(SUM(retailer_count), 0) AS avg_sku_discount_perc
    FROM sku_discount_by_tier
    GROUP BY product_id, warehouse_id
),

-- Quantity discounts active yesterday (average of tier discount %s)
qd_yesterday AS (
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
            MAX(CASE WHEN tier = 1 THEN discount_percentage END) AS t1_disc_pct,
            MAX(CASE WHEN tier = 2 THEN discount_percentage END) AS t2_disc_pct,
            MAX(CASE WHEN tier = 3 THEN discount_percentage END) AS t3_disc_pct
        FROM (
            SELECT
                qd.id,
                qdv.product_id,
                qdv.packing_unit_id,
                qdv.quantity,
                qdv.discount_percentage,
                qd.dynamic_tag_id,
                ROW_NUMBER() OVER (
                    PARTITION BY qdv.product_id, qdv.packing_unit_id, qd.id
                    ORDER BY qdv.quantity
                ) AS tier
            FROM quantity_discounts qd
            JOIN quantity_discount_values qdv ON qdv.quantity_discount_id = qd.id
            WHERE qd.active = TRUE
                AND CURRENT_DATE - 1 BETWEEN qd.start_at::date AND qd.end_at::date
        ) qd_tiers
        JOIN qd_det qd ON qd.tag_id = qd_tiers.dynamic_tag_id
        GROUP BY product_id, packing_unit_id, qd.warehouse_id
    )
    SELECT
        qc.product_id,
        qc.warehouse_id,
        AVG(
            (COALESCE(qc.t1_disc_pct, 0)
           + COALESCE(qc.t2_disc_pct, 0)
           + COALESCE(qc.t3_disc_pct, 0))
          / NULLIF(
                (CASE WHEN qc.t1_disc_pct IS NOT NULL THEN 1 ELSE 0 END
               + CASE WHEN qc.t2_disc_pct IS NOT NULL THEN 1 ELSE 0 END
               + CASE WHEN qc.t3_disc_pct IS NOT NULL THEN 1 ELSE 0 END), 0)
        ) AS avg_qty_discount_perc
    FROM qd_config qc
    JOIN PACKING_UNIT_PRODUCTS pup
        ON pup.product_id = qc.product_id
        AND pup.packing_unit_id = qc.packing_unit_id
    WHERE pup.basic_unit_count = 1
    GROUP BY qc.product_id, qc.warehouse_id
),

-- Yesterday's margins from actual sales (before and after discounts)
yesterday_margins AS (
    SELECT
        so.warehouse_id,
        pso.product_id,
        SUM(pso.total_price) AS gross_revenue,
        SUM(pd.wac_p * pso.purchased_item_count * pso.basic_unit_count) AS cost_before_discounts,
        SUM(pd.wac_p * pso.purchased_item_count * pso.basic_unit_count
            + COALESCE(pso.ITEM_DISCOUNT_VALUE, 0) * pso.purchased_item_count * pso.basic_unit_count
            + COALESCE(pso.ITEM_QUANTITY_DISCOUNT_VALUE, 0) * pso.purchased_item_count * pso.basic_unit_count
        ) AS cost_after_discounts
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    LEFT JOIN MATERIALIZED_VIEWS.Pricing_data_extraction pd
        ON pd.product_id = pso.product_id
        AND pd.warehouse_id = so.warehouse_id
        AND pd.created_at::date = CURRENT_DATE - 1
    WHERE so.created_at::date = CURRENT_DATE - 1
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
    GROUP BY so.warehouse_id, pso.product_id
),

-- Day-before margins from actual sales (mirror of yesterday_margins for D-2)
day_before_margins AS (
    SELECT
        so.warehouse_id,
        pso.product_id,
        SUM(pso.total_price) AS gross_revenue,
        SUM(pd.wac_p * pso.purchased_item_count * pso.basic_unit_count) AS cost_before_discounts,
        SUM(pd.wac_p * pso.purchased_item_count * pso.basic_unit_count
            + COALESCE(pso.ITEM_DISCOUNT_VALUE, 0) * pso.purchased_item_count * pso.basic_unit_count
            + COALESCE(pso.ITEM_QUANTITY_DISCOUNT_VALUE, 0) * pso.purchased_item_count * pso.basic_unit_count
        ) AS cost_after_discounts
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    LEFT JOIN MATERIALIZED_VIEWS.Pricing_data_extraction pd
        ON pd.product_id = pso.product_id
        AND pd.warehouse_id = so.warehouse_id
        AND pd.created_at::date = CURRENT_DATE - 2
    WHERE so.created_at::date = CURRENT_DATE - 2
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
    GROUP BY so.warehouse_id, pso.product_id
)

-- Final output
SELECT
    pi.product_id,
    pi.warehouse_id,
    pi.sku,
    pi.brand,
    pi.cat,
    pi.stocks,
    ROUND(pi.stocks * COALESCE(pi.wac_p, 0), 2) AS stock_value,

    COALESCE(sqdb.sold_qty, 0) AS qty_sold_day_before_yesterday,
    COALESCE(sqy.sold_qty, 0) AS qty_sold_yesterday,

    ROUND(sqy.avg_selling_price, 2) AS avg_selling_price_yesterday,
    ROUND(sqy.avg_selling_price_after_discount, 2) AS avg_selling_price_after_discount,
    pi.market_median,

    -- Price reduction percentage (positive value when price dropped)
    CASE
        WHEN scp.closing_price < scp.start_price
        THEN ROUND(((scp.start_price - scp.closing_price) / scp.start_price) * 100, 2)
        ELSE 0
    END AS price_reduction_perc,

    -- Price increase percentage (positive value when price rose)
    CASE
        WHEN scp.closing_price > scp.start_price
        THEN ROUND(((scp.closing_price - scp.start_price) / scp.start_price) * 100, 2)
        ELSE 0
    END AS price_increase_perc,

    -- Average SKU discount percentage (blended weighted)
    COALESCE(ROUND(skd.avg_sku_discount_perc, 2), 0) AS avg_sku_discount_perc,

    -- Average quantity discount percentage (mean of tier %s)
    COALESCE(ROUND(qd.avg_qty_discount_perc, 2), 0) AS avg_qty_discount_perc,

    -- Margin before discounts: (revenue - wac cost) / revenue
    CASE
        WHEN COALESCE(ym.gross_revenue, 0) > 0
        THEN ROUND(((ym.gross_revenue - COALESCE(ym.cost_before_discounts, 0)) / ym.gross_revenue) * 100, 2)
        ELSE NULL
    END AS margin_before_discounts,

    -- Margin after discounts: (revenue - (wac cost + discount cost)) / revenue
    CASE
        WHEN COALESCE(ym.gross_revenue, 0) > 0
        THEN ROUND(((ym.gross_revenue - COALESCE(ym.cost_after_discounts, 0)) / ym.gross_revenue) * 100, 2)
        ELSE NULL
    END AS margin_after_discounts,

    ROUND(ym.gross_revenue - COALESCE(ym.cost_after_discounts, 0), 2) AS gross_profit_yesterday,
    ROUND(dbm.gross_revenue - COALESCE(dbm.cost_after_discounts, 0), 2) AS gross_profit_day_before,

    ROUND((ym.gross_revenue - COALESCE(ym.cost_after_discounts, 0))
        - (dbm.gross_revenue - COALESCE(dbm.cost_after_discounts, 0)), 2) AS profit_delta,

    CASE
        WHEN ym.gross_revenue IS NULL OR dbm.gross_revenue IS NULL THEN NULL
        WHEN (ym.gross_revenue - COALESCE(ym.cost_after_discounts, 0))
           >= (dbm.gross_revenue - COALESCE(dbm.cost_after_discounts, 0))
        THEN 'Profitable'
        ELSE 'Not Profitable'
    END AS discount_profitability,

    CASE
        WHEN pi.market_median IS NULL OR sqy.avg_selling_price_after_discount IS NULL THEN NULL
        WHEN sqy.avg_selling_price_after_discount < pi.market_median * 0.99
             AND COALESCE(sqy.sold_qty, 0) < COALESCE(sqdb.sold_qty, 0) * 1.10
        THEN 'Over-Discounted'
        ELSE 'OK'
    END AS over_discounted_flag

FROM product_info pi
LEFT JOIN sold_qty_day_before sqdb
    ON sqdb.product_id = pi.product_id
    AND sqdb.warehouse_id = pi.warehouse_id
LEFT JOIN sold_qty_yesterday sqy
    ON sqy.product_id = pi.product_id
    AND sqy.warehouse_id = pi.warehouse_id
LEFT JOIN start_closing_prices scp
    ON scp.product_id = pi.product_id
    AND scp.warehouse_id = pi.warehouse_id
LEFT JOIN sku_discounts_yesterday skd
    ON skd.product_id = pi.product_id
    AND skd.warehouse_id = pi.warehouse_id
LEFT JOIN qd_yesterday qd
    ON qd.product_id = pi.product_id
    AND qd.warehouse_id = pi.warehouse_id
LEFT JOIN yesterday_margins ym
    ON ym.product_id = pi.product_id
    AND ym.warehouse_id = pi.warehouse_id
LEFT JOIN day_before_margins dbm
    ON dbm.product_id = pi.product_id
    AND dbm.warehouse_id = pi.warehouse_id
ORDER BY stock_value desc
