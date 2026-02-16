-- =============================================================================
-- LIVE PRICING VIEW QUERY
-- =============================================================================
-- This query provides a live view of pricing per SKU-warehouse, showing:
-- 1. Tag price (current price)
-- 2. Planned effective price (tag price - blended SKU discount)
-- 3. Actual effective price (from redeemed discounts in product_sales_order)
-- 4. QD tier prices (T1, T2, T3) - both planned and effective
-- =============================================================================

WITH 
-- Step 1: Get Tag Price (Current Price)
-- Get current price from cohort_product_packing_units for basic units
tag_prices AS (
    WITH local_prices AS (
        SELECT  
            CASE 
                WHEN cpu.cohort_id IN (700, 695) THEN 'Cairo'
                WHEN cpu.cohort_id IN (701) THEN 'Giza'
                WHEN cpu.cohort_id IN (704, 698) THEN 'Delta East'
                WHEN cpu.cohort_id IN (703, 697) THEN 'Delta West'
                WHEN cpu.cohort_id IN (696, 1123, 1124, 1125, 1126) THEN 'Upper Egypt'
                WHEN cpu.cohort_id IN (702, 699) THEN 'Alexandria'
            END AS region,
            cpu.cohort_id,
            pu.product_id,
            pu.packing_unit_id,
            pu.basic_unit_count,
            AVG(cpu.price) AS price
        FROM cohort_product_packing_units cpu
        JOIN PACKING_UNIT_PRODUCTS pu ON pu.id = cpu.product_packing_unit_id
        WHERE cpu.cohort_id IN (700,701,702,703,704,695,696,697,698,699,1123,1124,1125,1126)
            AND cpu.created_at::date <> '2023-07-31'
            AND cpu.is_customized = TRUE
            AND pu.basic_unit_count = 1
        GROUP BY ALL
    ),

    prices AS (
        SELECT *
        FROM (
            SELECT *, 2 AS priority FROM local_prices
        )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY region, cohort_id, product_id, packing_unit_id ORDER BY priority) = 1
    ),
    -- Map cohort to warehouse (specific mapping)
    cohort_warehouse_map AS (
        SELECT * FROM VALUES
        (700,1),
        (701,236),
        (701,962),
        (702,797),
        (703,337),
        (703,8),
        (704,339),
        (704,170),
        (1123,703),
        (1124,501),
        (1125,632),
        (1126,401)
   -- Alexandria -> Khorshed Alex
        AS x(cohort_id, warehouse_id)
    )
    SELECT DISTINCT
        p.cohort_id,
        cwm.warehouse_id,
        p.product_id,
        p.price AS tag_price
    FROM prices p
    JOIN cohort_warehouse_map cwm ON cwm.cohort_id = p.cohort_id
    WHERE p.basic_unit_count = 1
        AND ((p.product_id = 1309 AND p.packing_unit_id = 2) OR (p.product_id <> 1309))
),

-- Step 2: Get Active SKU Discounts with Retailer Mapping and Calculate Blended Discount
sku_discount_blended AS (
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
        ) f
        WHERE sd.name_en = 'Special Discounts'
            AND sd.active = 'true'
            AND sd.start_at <= CURRENT_TIMESTAMP()
            AND sd.end_at >= CURRENT_TIMESTAMP()
    ),
    sku_discount_warehouse_mapping AS (
        SELECT DISTINCT
            sdv.product_id,
            sdv.discount_percentage,
            wdr.warehouse_id,
            asd.retailer_id,
            asd.start_at AS discount_start_at
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
			discount_start_at,
            COUNT(DISTINCT retailer_id) AS retailer_count
			
        FROM sku_discount_warehouse_mapping
        GROUP BY all
    ),
    sku_discount_summary AS (
        SELECT 
            product_id,
            warehouse_id,
            -- Weighted average discount: SUM(discount * retailer_count) / SUM(retailer_count)
            SUM(discount_percentage * retailer_count) / NULLIF(SUM(retailer_count), 0) AS blended_sku_discount_pct,
            -- Get earliest start_at date for this product-warehouse (when discount offer started)
            MIN(discount_start_at) AS discount_start_at
        FROM sku_discount_by_tier
        GROUP BY product_id, warehouse_id
    )
    SELECT 
        product_id,
        warehouse_id,
        blended_sku_discount_pct,
        discount_start_at
    FROM sku_discount_summary
),

-- Step 3: Get Actual Effective Price from Sales Since Discount Offer Started
actual_effective_price AS (
    SELECT 
        sds.warehouse_id,
        sds.product_id,
        -- Actual effective price = (total_price - SKU discount) / (qty * basic_unit_count)
        -- Calculate from sales since the discount offer started
        AVG(
            (pso.total_price - COALESCE(pso.ITEM_DISCOUNT_VALUE*pso.purchased_item_count, 0)) 
            / NULLIF(pso.purchased_item_count * pso.basic_unit_count, 0)
        ) AS actual_effective_price
    FROM sku_discount_blended sds
    JOIN product_sales_order pso ON pso.product_id = sds.product_id
    JOIN sales_orders so ON so.id = pso.sales_order_id
        AND so.warehouse_id = sds.warehouse_id
    WHERE so.created_at >= sds.discount_start_at::date
        AND so.created_at <= CURRENT_TIMESTAMP()
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
    GROUP BY sds.warehouse_id, sds.product_id
),

-- Step 4: Get Active QD Tiers (T1, T2, T3)
qd_tiers AS (
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
        SELECT * 
        FROM (
            SELECT 
                product_id,
                packing_unit_id,
                qd.warehouse_id,
                qd_tiers.start_at AS discount_start_at,
                MAX(CASE WHEN tier = 1 THEN quantity END) AS qd_tier_1_qty,
                MAX(CASE WHEN tier = 1 THEN discount_percentage END) AS qd_tier_1_disc_pct,
                MAX(CASE WHEN tier = 2 THEN quantity END) AS qd_tier_2_qty,
                MAX(CASE WHEN tier = 2 THEN discount_percentage END) AS qd_tier_2_disc_pct,
                MAX(CASE WHEN tier = 3 THEN quantity END) AS qd_tier_3_qty,
                MAX(CASE WHEN tier = 3 THEN discount_percentage END) AS qd_tier_3_disc_pct
            FROM (
                SELECT 
                    qd.id,
                    qdv.product_id,
                    qdv.packing_unit_id,
                    qdv.quantity,
                    qdv.discount_percentage,
                    qd.dynamic_tag_id,
                    qd.start_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY qdv.product_id, qdv.packing_unit_id, qd.id 
                        ORDER BY qdv.quantity
                    ) AS tier
                FROM quantity_discounts qd 
                JOIN quantity_discount_values qdv ON qdv.quantity_discount_id = qd.id
                WHERE qd.active = TRUE
                    AND qd.start_at <= CURRENT_TIMESTAMP()
                    AND qd.end_at >= CURRENT_TIMESTAMP()
            ) qd_tiers
            JOIN qd_det qd ON qd.tag_id = qd_tiers.dynamic_tag_id
            GROUP BY ALL
        )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, packing_unit_id, warehouse_id ORDER BY qd_tier_1_qty DESC) = 1
    )
    -- Convert to basic unit level (filter to basic_unit_count = 1) and aggregate by product-warehouse
    SELECT 
        qc.product_id,
        qc.warehouse_id,
        qc.qd_tier_1_qty AS qd_t1_qty,
        qc.qd_tier_1_disc_pct AS qd_t1_discount_pct,
        qc.qd_tier_2_qty AS qd_t2_qty,
        qc.qd_tier_2_disc_pct AS qd_t2_discount_pct,
        qc.qd_tier_3_qty AS qd_t3_qty,
        qc.qd_tier_3_disc_pct AS qd_t3_discount_pct,
        MIN(qc.discount_start_at) AS discount_start_at
    FROM qd_config qc
    JOIN PACKING_UNIT_PRODUCTS pup ON pup.product_id = qc.product_id 
        AND pup.packing_unit_id = qc.packing_unit_id
    WHERE pup.basic_unit_count = 1
    GROUP BY 
        qc.product_id,
        qc.warehouse_id,
        qc.qd_tier_1_qty,
        qc.qd_tier_1_disc_pct,
        qc.qd_tier_2_qty,
        qc.qd_tier_2_disc_pct,
        qc.qd_tier_3_qty,
        qc.qd_tier_3_disc_pct
),

-- Step 5: Get QD Effective Prices from Sales Since QD Offer Started
qd_effective_prices AS (
    WITH sales_with_tiers AS (
        SELECT 
            qt.warehouse_id,
            qt.product_id,
            pso.purchased_item_count AS qty,
            pso.total_price,
            pso.ITEM_QUANTITY_DISCOUNT_VALUE,
            pso.basic_unit_count,
            qt.qd_t1_qty,
            qt.qd_t2_qty,
            qt.qd_t3_qty,
            qt.discount_start_at
        FROM qd_tiers qt
        JOIN product_sales_order pso ON pso.product_id = qt.product_id
        JOIN sales_orders so ON so.id = pso.sales_order_id
            AND so.warehouse_id = qt.warehouse_id
        WHERE so.created_at >= qt.discount_start_at::date
            AND so.created_at <= CURRENT_TIMESTAMP()
            AND so.sales_order_status_id NOT IN (7, 12)
            AND so.channel IN ('telesales', 'retailer')
            AND pso.purchased_item_count <> 0
    )
    SELECT 
        warehouse_id,
        product_id,
        -- T1 effective price: price after QD discount for orders in T1 range
        AVG(
            CASE 
                WHEN qd_t1_qty IS NOT NULL 
                     AND qty >= qd_t1_qty 
                     AND (qd_t2_qty IS NULL OR qty < qd_t2_qty)
                THEN (total_price - ITEM_QUANTITY_DISCOUNT_VALUE*qty) / NULLIF(qty * basic_unit_count, 0)
                ELSE NULL
            END
        ) AS qd_t1_effective_price,
        -- T2 effective price: price after QD discount for orders in T2 range
        AVG(
            CASE 
                WHEN qd_t2_qty IS NOT NULL 
                     AND qty >= qd_t2_qty 
                     AND (qd_t3_qty IS NULL OR qty < qd_t3_qty)
                THEN (total_price - ITEM_QUANTITY_DISCOUNT_VALUE*qty) / NULLIF(qty * basic_unit_count, 0)
                ELSE NULL
            END
        ) AS qd_t2_effective_price,
        -- T3 effective price: price after QD discount for orders in T3 range
        AVG(
            CASE 
                WHEN qd_t3_qty IS NOT NULL 
                     AND qty >= qd_t3_qty
                THEN (total_price - ITEM_QUANTITY_DISCOUNT_VALUE*qty) / NULLIF(qty * basic_unit_count, 0)
                ELSE NULL
            END
        ) AS qd_t3_effective_price
    FROM sales_with_tiers
    GROUP BY warehouse_id, product_id
),

-- Step 6: Combine All Data
combined_data AS (
    SELECT 
        tp.product_id,
        tp.warehouse_id,
        tp.cohort_id,
        tp.tag_price,
        COALESCE(sdb.blended_sku_discount_pct, 0) AS blended_sku_discount_pct,
        -- Planned effective price = tag_price * (1 - blended_sku_discount_pct / 100)
        tp.tag_price * (1 - COALESCE(sdb.blended_sku_discount_pct, 0) / 100) AS planned_effective_price,
        aep.actual_effective_price,
        qt.qd_t1_qty,
        qt.qd_t1_discount_pct,
        qt.qd_t2_qty,
        qt.qd_t2_discount_pct,
        qt.qd_t3_qty,
        qt.qd_t3_discount_pct,
        -- QD Tier Planned Prices
        CASE 
            WHEN qt.qd_t1_discount_pct IS NOT NULL 
            THEN tp.tag_price * (1 - qt.qd_t1_discount_pct / 100)
            ELSE NULL
        END AS qd_t1_planned_price,
        CASE 
            WHEN qt.qd_t2_discount_pct IS NOT NULL 
            THEN tp.tag_price * (1 - qt.qd_t2_discount_pct / 100)
            ELSE NULL
        END AS qd_t2_planned_price,
        CASE 
            WHEN qt.qd_t3_discount_pct IS NOT NULL 
            THEN tp.tag_price * (1 - qt.qd_t3_discount_pct / 100)
            ELSE NULL
        END AS qd_t3_planned_price,
        -- QD Tier Effective Prices
        qep.qd_t1_effective_price,
        qep.qd_t2_effective_price,
        qep.qd_t3_effective_price
    FROM tag_prices tp
    LEFT JOIN sku_discount_blended sdb ON sdb.product_id = tp.product_id 
        AND sdb.warehouse_id = tp.warehouse_id
    LEFT JOIN actual_effective_price aep ON aep.product_id = tp.product_id 
        AND aep.warehouse_id = tp.warehouse_id
    LEFT JOIN qd_tiers qt ON qt.product_id = tp.product_id 
        AND qt.warehouse_id = tp.warehouse_id
    LEFT JOIN qd_effective_prices qep ON qep.product_id = tp.product_id 
        AND qep.warehouse_id = tp.warehouse_id
)

-- Final Output
SELECT 
    cd.product_id,
    cd.warehouse_id,
    cd.cohort_id,
    p.name_ar || ' ' || p.size || ' ' || pu.name_ar AS sku,
    b.name_ar AS brand,
    cat.name_ar AS cat,
    cd.tag_price,
    cd.blended_sku_discount_pct,
    cd.planned_effective_price,
    cd.actual_effective_price,
    cd.qd_t1_qty,
    cd.qd_t1_discount_pct,
    cd.qd_t1_planned_price,
    cd.qd_t1_effective_price,
    cd.qd_t2_qty,
    cd.qd_t2_discount_pct,
    cd.qd_t2_planned_price,
    cd.qd_t2_effective_price,
    cd.qd_t3_qty,
    cd.qd_t3_discount_pct,
    cd.qd_t3_planned_price,
    cd.qd_t3_effective_price
FROM combined_data cd
JOIN products p ON p.id = cd.product_id
JOIN brands b ON b.id = p.brand_id
JOIN categories cat ON cat.id = p.category_id
JOIN product_units pu ON pu.id = p.unit_id
where product_id = 130
ORDER BY cd.product_id, cd.warehouse_id;

