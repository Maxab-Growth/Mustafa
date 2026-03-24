-- =============================================================================
-- MARKET POSITION QUERIES
-- =============================================================================
-- These queries provide market price position analysis for SKUs
-- Based on market_data_module.ipynb logic
-- 
-- Queries:
-- 1. MARKET_POSITION_PER_SKU: Price position per SKU (above, below, median, etc.)
-- 2. MARKET_POSITION_WEIGHTED_YESTERDAY: Weighted by yesterday's NMV
-- 3. MARKET_POSITION_WEIGHTED_TODAY: Weighted by today's NMV
-- =============================================================================


-- =============================================================================
-- QUERY 1: MARKET POSITION PER SKU
-- =============================================================================
-- Returns each SKU's current price position relative to market prices
-- Positions: below_market, at_min, at_25pct, at_median, at_75pct, at_max, above_market

WITH 
-- Ben Soliman Prices
ben_soliman AS (
    WITH lower AS (
        SELECT DISTINCT product_id, new_d*bs_price AS ben_soliman_price, INJECTION_DATE
        FROM (
            SELECT maxab_product_id AS product_id, INJECTION_DATE, wac1, wac_p,
                (bs_price) AS bs_price, diff, cu_price,
                CASE WHEN p1 > 1 THEN child_quantity ELSE 0 END AS scheck,
                ROUND(p1/2)*2 AS p1, p2,
                CASE WHEN (ROUND(p1 / scheck) * scheck) = 0 THEN p1 ELSE (ROUND(p1 / scheck) * scheck) END AS new_d
            FROM (
                SELECT sm.*, wac1, wac_p, 
                    ABS((bs_price)-(wac_p*maxab_basic_unit_count))/(wac_p*maxab_basic_unit_count) AS diff,
                    cpc.price AS cu_price, pup.child_quantity,
                    ROUND((cu_price/bs_price)) AS p1, 
                    ROUND(((bs_price)/cu_price)) AS p2
                FROM materialized_views.savvy_mapping sm 
                JOIN finance.all_cogs f ON f.product_id = sm.maxab_product_id 
                    AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_Date AND f.to_date
                JOIN PACKING_UNIT_PRODUCTS pu ON pu.product_id = sm.maxab_product_id AND pu.IS_BASIC_UNIT = 1 
                JOIN cohort_product_packing_units cpc ON cpc.PRODUCT_PACKING_UNIT_ID = pu.id AND cohort_id = 700 
                JOIN packing_unit_products pup ON pup.product_id = sm.maxab_product_id AND pup.is_basic_unit = 1  
                WHERE bs_price IS NOT NULL 
                    AND INJECTION_DATE::date >= CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP())::date - 5 
                    AND diff > 0.3 AND p1 > 1
            )
        )
        QUALIFY MAX(INJECTION_DATE) OVER(PARTITION BY product_id) = INJECTION_DATE
    ),
    m_bs AS (
        SELECT z.* FROM (
            SELECT maxab_product_id AS product_id, AVG(bs_final_price) AS ben_soliman_price, INJECTION_DATE
            FROM (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY maxab_product_id ORDER BY diff) AS rnk_2 
                FROM (
                    SELECT *, (bs_final_price-wac_p)/wac_p AS diff_2 
                    FROM (
                        SELECT *, bs_price/maxab_basic_unit_count AS bs_final_price 
                        FROM (
                            SELECT *, ROW_NUMBER() OVER(PARTITION BY maxab_product_id, maxab_pu ORDER BY diff) AS rnk 
                            FROM (
                                SELECT *, MAX(INJECTION_DATE::date) OVER(PARTITION BY maxab_product_id, maxab_pu) AS max_date
                                FROM (
                                    SELECT sm.*, wac1, wac_p, 
                                        ABS(bs_price-(wac_p*maxab_basic_unit_count))/(wac_p*maxab_basic_unit_count) AS diff 
                                    FROM materialized_views.savvy_mapping sm 
                                    JOIN finance.all_cogs f ON f.product_id = sm.maxab_product_id 
                                        AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_Date AND f.to_date
                                    WHERE bs_price IS NOT NULL 
                                        AND INJECTION_DATE::date >= CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP())::date - 5 
                                        AND diff < 0.3
                                )
                                QUALIFY max_date = INJECTION_DATE
                            ) QUALIFY rnk = 1 
                        )
                    ) WHERE diff_2 BETWEEN -0.5 AND 0.5 
                ) QUALIFY rnk_2 = 1 
            ) GROUP BY ALL
        ) z 
        JOIN finance.all_cogs f ON f.product_id = z.product_id 
            AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_Date AND f.to_date
        WHERE ben_soliman_price BETWEEN f.wac_p*0.8 AND f.wac_p*1.3
    )
    SELECT product_id, AVG(ben_soliman_price) AS ben_soliman_price
    FROM (
        SELECT product_id, ben_soliman_price, INJECTION_DATE
        FROM (
            SELECT * FROM (
                SELECT *, 1 AS prio FROM m_bs 
                UNION ALL
                SELECT *, 2 AS prio FROM lower
            )
            QUALIFY MAX(INJECTION_DATE) OVER(PARTITION BY product_id) = INJECTION_DATE
        )
        QUALIFY prio = MIN(prio) OVER(PARTITION BY product_id)
    )
    GROUP BY ALL
),

-- Marketplace Prices with Region Fallback
marketplace_prices AS (
    WITH MP AS (
        SELECT region, product_id,
            MIN(min_price) AS min_price, MIN(max_price) AS max_price,
            MIN(mod_price) AS mod_price, MIN(true_min) AS true_min, MIN(true_max) AS true_max
        FROM (
            SELECT mp.region, mp.product_id, mp.pu_id,
                min_price/BASIC_UNIT_COUNT AS min_price,
                max_price/BASIC_UNIT_COUNT AS max_price,
                mod_price/BASIC_UNIT_COUNT AS mod_price,
                TRUE_MIN_PRICE/BASIC_UNIT_COUNT AS true_min,
                TRUE_MAX_PRICE/BASIC_UNIT_COUNT AS true_max
            FROM materialized_views.marketplace_prices mp 
            JOIN packing_unit_products pup ON pup.product_id = mp.product_id AND pup.packing_unit_id = mp.pu_id
            JOIN finance.all_cogs f ON f.product_id = mp.product_id 
                AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_date AND f.to_date
            WHERE LEAST(min_price, mod_price) BETWEEN wac_p*0.9 AND wac_p*1.3 
        )
        GROUP BY ALL 
    ),
    region_mapping AS (
        SELECT * FROM (VALUES
            ('Delta East', 'Delta West'), ('Delta West', 'Delta East'),
            ('Alexandria', 'Cairo'), ('Alexandria', 'Giza'),
            ('Upper Egypt', 'Cairo'), ('Upper Egypt', 'Giza'),
            ('Cairo', 'Giza'), ('Giza', 'Cairo'),
            ('Delta West', 'Cairo'), ('Delta East', 'Cairo'),
            ('Delta West', 'Giza'), ('Delta East', 'Giza')
        ) AS region_mapping(region, fallback_region)
    ),
    all_regions AS (
        SELECT * FROM (VALUES
            ('Cairo'), ('Giza'), ('Delta West'), ('Delta East'), ('Upper Egypt'), ('Alexandria')
        ) AS x(region)
    ),
    full_data AS (
        SELECT products.id AS product_id, ar.region
        FROM products, all_regions ar
        WHERE activation = 'true'
    )
    SELECT region, product_id,
        MIN(final_min_price) AS final_min_price, 
        MIN(final_max_price) AS final_max_price,
        MIN(final_mod_price) AS final_mod_price, 
        MIN(final_true_min) AS final_true_min,
        MIN(final_true_max) AS final_true_max
    FROM (
        SELECT DISTINCT w.region, w.product_id,
            COALESCE(m1.min_price, m2.min_price) AS final_min_price,
            COALESCE(m1.max_price, m2.max_price) AS final_max_price,
            COALESCE(m1.mod_price, m2.mod_price) AS final_mod_price,
            COALESCE(m1.true_min, m2.true_min) AS final_true_min,
            COALESCE(m1.true_max, m2.true_max) AS final_true_max
        FROM full_data w
        LEFT JOIN MP m1 ON w.region = m1.region AND w.product_id = m1.product_id
        LEFT JOIN region_mapping rm ON w.region = rm.region
        LEFT JOIN MP m2 ON rm.fallback_region = m2.region AND w.product_id = m2.product_id
    )
    WHERE final_min_price IS NOT NULL 
    GROUP BY ALL
),

-- Scrapped Prices (Competitor prices)
scrapped_prices AS (
    SELECT product_id, region,
        MIN(market_price) AS min_scrapped,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY market_price) AS scrapped25,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY market_price) AS scrapped50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY market_price) AS scrapped75,
        MAX(market_price) AS max_scrapped
    FROM (
        SELECT DISTINCT cmp.*, MAX(date) OVER(PARTITION BY region, cmp.product_id, competitor) AS max_date
        FROM MATERIALIZED_VIEWS.CLEANED_MARKET_PRICES cmp
        JOIN finance.all_cogs f ON f.product_id = cmp.product_id 
            AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_date AND f.to_date 
        WHERE date >= CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP())::date - 7 
            AND MARKET_PRICE BETWEEN f.wac_p * 0.8 AND wac_p * 1.3
        QUALIFY date = max_date 
    )
    GROUP BY ALL
),

-- Product Base (WAC and product info)
product_base AS (
    SELECT DISTINCT
        CASE 
            WHEN cohort_id IN (700, 695) THEN 'Cairo'
            WHEN cohort_id IN (701) THEN 'Giza'
            WHEN cohort_id IN (704, 698) THEN 'Delta East'
            WHEN cohort_id IN (703, 697) THEN 'Delta West'
            WHEN cohort_id IN (696, 1123, 1124, 1125, 1126) THEN 'Upper Egypt'
            WHEN cohort_id IN (702, 699) THEN 'Alexandria'
        END AS region,
        cohort_id,
        f.product_id,
        CONCAT(products.name_ar, ' ', products.size, ' ', product_units.name_ar) AS sku_name,
        brands.name_ar AS brand,
        categories.name_ar AS cat,
        f.wac1,
        f.wac_p
    FROM finance.all_cogs f
    JOIN products ON products.id = f.product_id
    JOIN brands ON products.brand_id = brands.id
    JOIN categories ON products.category_id = categories.id
    JOIN product_units ON product_units.id = products.unit_id
    CROSS JOIN (
        SELECT DISTINCT cohort_id 
        FROM COHORT_PRICING_CHANGES 
        WHERE cohort_id IN (700,701,702,703,704,1123,1124,1125,1126)
    ) cohorts
    WHERE CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_date AND f.to_date
        AND products.activation = 'true'
),

-- Current Prices
current_prices AS (
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
            cohort_id,
            pu.product_id,
            pu.packing_unit_id,
            pu.basic_unit_count,
            AVG(cpu.price) AS price
        FROM cohort_product_packing_units cpu
        JOIN PACKING_UNIT_PRODUCTS pu ON pu.id = cpu.product_packing_unit_id
        WHERE cpu.cohort_id IN (700,701,702,703,704,695,696,697,698,699,1123,1124,1125,1126)
            AND cpu.created_at::date <> '2023-07-31'
            AND cpu.is_customized = TRUE
        GROUP BY ALL
    ),
    live_prices AS (
        SELECT 
            region, cohort_id, product_id, 
            pu_id AS packing_unit_id, 
            buc AS basic_unit_count, 
            NEW_PRICE AS price
        FROM materialized_views.DBDP_PRICES
        WHERE created_at = CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP())::date
            AND DATE_PART('hour', CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP())::time) 
                BETWEEN SPLIT_PART(time_slot, '-', 1)::int AND (SPLIT_PART(time_slot, '-', 1)::int) + 1
            AND cohort_id IN (700,701,702,703,704,695,696,697,698,699,1123,1124,1125,1126)
    ),
    prices AS (
        SELECT *
        FROM (
            SELECT *, 1 AS priority FROM live_prices
            UNION ALL
            SELECT *, 2 AS priority FROM local_prices
        )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY region, cohort_id, product_id, packing_unit_id ORDER BY priority) = 1
    )
    SELECT region, cohort_id, product_id, price AS current_price
    FROM prices
    WHERE basic_unit_count = 1
        AND ((product_id = 1309 AND packing_unit_id = 2) OR (product_id <> 1309))
),

-- Combine all market data
market_data_combined AS (
    SELECT 
        pb.region,
        pb.cohort_id,
        pb.product_id,
        pb.sku_name,
        pb.brand,
        pb.cat,
        pb.wac_p,
        cp.current_price,
        bs.ben_soliman_price,
        mp.final_min_price,
        mp.final_max_price,
        mp.final_mod_price,
        sp.min_scrapped,
        sp.scrapped25,
        sp.scrapped50,
        sp.scrapped75,
        sp.max_scrapped
    FROM product_base pb
    LEFT JOIN current_prices cp ON pb.cohort_id = cp.cohort_id AND pb.product_id = cp.product_id
    LEFT JOIN ben_soliman bs ON pb.product_id = bs.product_id
    LEFT JOIN marketplace_prices mp ON pb.region = mp.region AND pb.product_id = mp.product_id
    LEFT JOIN scrapped_prices sp ON pb.region = sp.region AND pb.product_id = sp.product_id
),

-- Calculate market price percentiles
market_percentiles AS (
    SELECT 
        *,
        -- Calculate minimum price from all sources
        LEAST(
            COALESCE(ben_soliman_price, 999999),
            COALESCE(final_min_price, 999999),
            COALESCE(min_scrapped, 999999)
        ) AS market_minimum,
        -- Calculate 25th percentile (approximate)
        COALESCE(scrapped25, final_min_price, ben_soliman_price) AS market_25pct,
        -- Calculate median (50th percentile)
        COALESCE(scrapped50, final_mod_price, ben_soliman_price) AS market_median,
        -- Calculate 75th percentile
        COALESCE(scrapped75, final_max_price, ben_soliman_price) AS market_75pct,
        -- Calculate maximum price from all sources
        GREATEST(
            COALESCE(ben_soliman_price, 0),
            COALESCE(final_max_price, 0),
            COALESCE(max_scrapped, 0)
        ) AS market_maximum
    FROM market_data_combined
    WHERE -- Filter: must have at least some market data
        ben_soliman_price IS NOT NULL 
        OR final_min_price IS NOT NULL 
        OR min_scrapped IS NOT NULL
),

-- Final market position calculation
final_market_position AS (
    SELECT 
        region,
        cohort_id,
        product_id,
        sku_name,
        brand,
        cat,
        wac_p,
        current_price,
        -- Market prices
        market_minimum,
        market_25pct,
        market_median,
        market_75pct,
        market_maximum,
        -- Current margin
        CASE WHEN current_price > 0 THEN (current_price - wac_p) / current_price ELSE 0 END AS current_margin,
        -- Market position determination
        CASE 
            WHEN current_price IS NULL OR current_price = 0 THEN 'no_price'
            WHEN market_minimum IS NULL OR market_minimum = 999999 THEN 'no_market_data'
            WHEN current_price < market_minimum * 0.98 THEN 'below_market'
            WHEN current_price <= market_minimum * 1.02 THEN 'at_market_min'
            WHEN current_price <= market_25pct * 1.02 THEN 'at_25_percentile'
            WHEN current_price <= market_median * 1.02 THEN 'at_median'
            WHEN current_price <= market_75pct * 1.02 THEN 'at_75_percentile'
            WHEN current_price <= market_maximum * 1.02 THEN 'at_market_max'
            ELSE 'above_market'
        END AS market_position,
        -- Price gap from median (%)
        CASE 
            WHEN market_median > 0 AND current_price > 0 
            THEN (current_price - market_median) / market_median * 100 
            ELSE NULL 
        END AS pct_gap_from_median,
        -- Price gap from minimum (%)
        CASE 
            WHEN market_minimum > 0 AND market_minimum < 999999 AND current_price > 0 
            THEN (current_price - market_minimum) / market_minimum * 100 
            ELSE NULL 
        END AS pct_gap_from_min
    FROM market_percentiles
    WHERE market_minimum < 999999  -- Has valid market data
)

SELECT * FROM final_market_position
ORDER BY region, brand, cat, sku_name;


-- =============================================================================
-- QUERY 2: MARKET POSITION WEIGHTED BY YESTERDAY'S NMV
-- =============================================================================
-- Returns weighted distribution of market positions based on yesterday's NMV

WITH 
-- [Same CTEs as above: ben_soliman, marketplace_prices, scrapped_prices, product_base, current_prices, market_data_combined, market_percentiles, final_market_position]
-- Ben Soliman Prices
ben_soliman AS (
    WITH lower AS (
        SELECT DISTINCT product_id, new_d*bs_price AS ben_soliman_price, INJECTION_DATE
        FROM (
            SELECT maxab_product_id AS product_id, INJECTION_DATE, wac1, wac_p,
                (bs_price) AS bs_price, diff, cu_price,
                CASE WHEN p1 > 1 THEN child_quantity ELSE 0 END AS scheck,
                ROUND(p1/2)*2 AS p1, p2,
                CASE WHEN (ROUND(p1 / scheck) * scheck) = 0 THEN p1 ELSE (ROUND(p1 / scheck) * scheck) END AS new_d
            FROM (
                SELECT sm.*, wac1, wac_p, 
                    ABS((bs_price)-(wac_p*maxab_basic_unit_count))/(wac_p*maxab_basic_unit_count) AS diff,
                    cpc.price AS cu_price, pup.child_quantity,
                    ROUND((cu_price/bs_price)) AS p1, 
                    ROUND(((bs_price)/cu_price)) AS p2
                FROM materialized_views.savvy_mapping sm 
                JOIN finance.all_cogs f ON f.product_id = sm.maxab_product_id 
                    AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_Date AND f.to_date
                JOIN PACKING_UNIT_PRODUCTS pu ON pu.product_id = sm.maxab_product_id AND pu.IS_BASIC_UNIT = 1 
                JOIN cohort_product_packing_units cpc ON cpc.PRODUCT_PACKING_UNIT_ID = pu.id AND cohort_id = 700 
                JOIN packing_unit_products pup ON pup.product_id = sm.maxab_product_id AND pup.is_basic_unit = 1  
                WHERE bs_price IS NOT NULL 
                    AND INJECTION_DATE::date >= CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP())::date - 5 
                    AND diff > 0.3 AND p1 > 1
            )
        )
        QUALIFY MAX(INJECTION_DATE) OVER(PARTITION BY product_id) = INJECTION_DATE
    ),
    m_bs AS (
        SELECT z.* FROM (
            SELECT maxab_product_id AS product_id, AVG(bs_final_price) AS ben_soliman_price, INJECTION_DATE
            FROM (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY maxab_product_id ORDER BY diff) AS rnk_2 
                FROM (
                    SELECT *, (bs_final_price-wac_p)/wac_p AS diff_2 
                    FROM (
                        SELECT *, bs_price/maxab_basic_unit_count AS bs_final_price 
                        FROM (
                            SELECT *, ROW_NUMBER() OVER(PARTITION BY maxab_product_id, maxab_pu ORDER BY diff) AS rnk 
                            FROM (
                                SELECT *, MAX(INJECTION_DATE::date) OVER(PARTITION BY maxab_product_id, maxab_pu) AS max_date
                                FROM (
                                    SELECT sm.*, wac1, wac_p, 
                                        ABS(bs_price-(wac_p*maxab_basic_unit_count))/(wac_p*maxab_basic_unit_count) AS diff 
                                    FROM materialized_views.savvy_mapping sm 
                                    JOIN finance.all_cogs f ON f.product_id = sm.maxab_product_id 
                                        AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_Date AND f.to_date
                                    WHERE bs_price IS NOT NULL 
                                        AND INJECTION_DATE::date >= CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP())::date - 5 
                                        AND diff < 0.3
                                )
                                QUALIFY max_date = INJECTION_DATE
                            ) QUALIFY rnk = 1 
                        )
                    ) WHERE diff_2 BETWEEN -0.5 AND 0.5 
                ) QUALIFY rnk_2 = 1 
            ) GROUP BY ALL
        ) z 
        JOIN finance.all_cogs f ON f.product_id = z.product_id 
            AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_Date AND f.to_date
        WHERE ben_soliman_price BETWEEN f.wac_p*0.8 AND f.wac_p*1.3
    )
    SELECT product_id, AVG(ben_soliman_price) AS ben_soliman_price
    FROM (
        SELECT product_id, ben_soliman_price, INJECTION_DATE
        FROM (
            SELECT * FROM (
                SELECT *, 1 AS prio FROM m_bs 
                UNION ALL
                SELECT *, 2 AS prio FROM lower
            )
            QUALIFY MAX(INJECTION_DATE) OVER(PARTITION BY product_id) = INJECTION_DATE
        )
        QUALIFY prio = MIN(prio) OVER(PARTITION BY product_id)
    )
    GROUP BY ALL
),

marketplace_prices AS (
    WITH MP AS (
        SELECT region, product_id,
            MIN(min_price) AS min_price, MIN(max_price) AS max_price,
            MIN(mod_price) AS mod_price, MIN(true_min) AS true_min, MIN(true_max) AS true_max
        FROM (
            SELECT mp.region, mp.product_id, mp.pu_id,
                min_price/BASIC_UNIT_COUNT AS min_price,
                max_price/BASIC_UNIT_COUNT AS max_price,
                mod_price/BASIC_UNIT_COUNT AS mod_price,
                TRUE_MIN_PRICE/BASIC_UNIT_COUNT AS true_min,
                TRUE_MAX_PRICE/BASIC_UNIT_COUNT AS true_max
            FROM materialized_views.marketplace_prices mp 
            JOIN packing_unit_products pup ON pup.product_id = mp.product_id AND pup.packing_unit_id = mp.pu_id
            JOIN finance.all_cogs f ON f.product_id = mp.product_id 
                AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_date AND f.to_date
            WHERE LEAST(min_price, mod_price) BETWEEN wac_p*0.9 AND wac_p*1.3 
        )
        GROUP BY ALL 
    ),
    region_mapping AS (
        SELECT * FROM (VALUES
            ('Delta East', 'Delta West'), ('Delta West', 'Delta East'),
            ('Alexandria', 'Cairo'), ('Alexandria', 'Giza'),
            ('Upper Egypt', 'Cairo'), ('Upper Egypt', 'Giza'),
            ('Cairo', 'Giza'), ('Giza', 'Cairo'),
            ('Delta West', 'Cairo'), ('Delta East', 'Cairo'),
            ('Delta West', 'Giza'), ('Delta East', 'Giza')
        ) AS region_mapping(region, fallback_region)
    ),
    all_regions AS (
        SELECT * FROM (VALUES
            ('Cairo'), ('Giza'), ('Delta West'), ('Delta East'), ('Upper Egypt'), ('Alexandria')
        ) AS x(region)
    ),
    full_data AS (
        SELECT products.id AS product_id, ar.region
        FROM products, all_regions ar
        WHERE activation = 'true'
    )
    SELECT region, product_id,
        MIN(final_min_price) AS final_min_price, 
        MIN(final_max_price) AS final_max_price,
        MIN(final_mod_price) AS final_mod_price
    FROM (
        SELECT DISTINCT w.region, w.product_id,
            COALESCE(m1.min_price, m2.min_price) AS final_min_price,
            COALESCE(m1.max_price, m2.max_price) AS final_max_price,
            COALESCE(m1.mod_price, m2.mod_price) AS final_mod_price
        FROM full_data w
        LEFT JOIN MP m1 ON w.region = m1.region AND w.product_id = m1.product_id
        LEFT JOIN region_mapping rm ON w.region = rm.region
        LEFT JOIN MP m2 ON rm.fallback_region = m2.region AND w.product_id = m2.product_id
    )
    WHERE final_min_price IS NOT NULL 
    GROUP BY ALL
),

scrapped_prices AS (
    SELECT product_id, region,
        MIN(market_price) AS min_scrapped,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY market_price) AS scrapped25,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY market_price) AS scrapped50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY market_price) AS scrapped75,
        MAX(market_price) AS max_scrapped
    FROM (
        SELECT DISTINCT cmp.*, MAX(date) OVER(PARTITION BY region, cmp.product_id, competitor) AS max_date
        FROM MATERIALIZED_VIEWS.CLEANED_MARKET_PRICES cmp
        JOIN finance.all_cogs f ON f.product_id = cmp.product_id 
            AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_date AND f.to_date 
        WHERE date >= CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP())::date - 7 
            AND MARKET_PRICE BETWEEN f.wac_p * 0.8 AND wac_p * 1.3
        QUALIFY date = max_date 
    )
    GROUP BY ALL
),

product_base AS (
    SELECT DISTINCT
        CASE 
            WHEN cohort_id IN (700, 695) THEN 'Cairo'
            WHEN cohort_id IN (701) THEN 'Giza'
            WHEN cohort_id IN (704, 698) THEN 'Delta East'
            WHEN cohort_id IN (703, 697) THEN 'Delta West'
            WHEN cohort_id IN (696, 1123, 1124, 1125, 1126) THEN 'Upper Egypt'
            WHEN cohort_id IN (702, 699) THEN 'Alexandria'
        END AS region,
        cohort_id,
        f.product_id,
        CONCAT(products.name_ar, ' ', products.size, ' ', product_units.name_ar) AS sku_name,
        brands.name_ar AS brand,
        categories.name_ar AS cat,
        f.wac_p
    FROM finance.all_cogs f
    JOIN products ON products.id = f.product_id
    JOIN brands ON products.brand_id = brands.id
    JOIN categories ON products.category_id = categories.id
    JOIN product_units ON product_units.id = products.unit_id
    CROSS JOIN (
        SELECT DISTINCT cohort_id 
        FROM COHORT_PRICING_CHANGES 
        WHERE cohort_id IN (700,701,702,703,704,1123,1124,1125,1126)
    ) cohorts
    WHERE CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_date AND f.to_date
        AND products.activation = 'true'
),

current_prices AS (
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
            cohort_id,
            pu.product_id,
            pu.basic_unit_count,
            AVG(cpu.price) AS price
        FROM cohort_product_packing_units cpu
        JOIN PACKING_UNIT_PRODUCTS pu ON pu.id = cpu.product_packing_unit_id
        WHERE cpu.cohort_id IN (700,701,702,703,704,695,696,697,698,699,1123,1124,1125,1126)
            AND cpu.created_at::date <> '2023-07-31'
            AND cpu.is_customized = TRUE
        GROUP BY ALL
    )
    SELECT region, cohort_id, product_id, price AS current_price
    FROM local_prices
    WHERE basic_unit_count = 1
),

-- Yesterday's NMV
yesterday_nmv AS (
    SELECT 
        cpc.cohort_id, 
        pso.product_id,
        SUM(pso.total_price) AS nmv
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    JOIN COHORT_PRICING_CHANGES cpc ON cpc.id = pso.COHORT_PRICING_CHANGE_id
    WHERE so.created_at::date = CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP())::date - 1
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
        AND cpc.cohort_id IN (700,701,702,703,704,1123,1124,1125,1126)
    GROUP BY ALL
),

market_data_combined AS (
    SELECT 
        pb.region,
        pb.cohort_id,
        pb.product_id,
        pb.sku_name,
        pb.brand,
        pb.cat,
        pb.wac_p,
        cp.current_price,
        bs.ben_soliman_price,
        mp.final_min_price,
        mp.final_max_price,
        mp.final_mod_price,
        sp.min_scrapped,
        sp.scrapped25,
        sp.scrapped50,
        sp.scrapped75,
        sp.max_scrapped,
        COALESCE(yn.nmv, 0) AS yesterday_nmv
    FROM product_base pb
    LEFT JOIN current_prices cp ON pb.cohort_id = cp.cohort_id AND pb.product_id = cp.product_id
    LEFT JOIN ben_soliman bs ON pb.product_id = bs.product_id
    LEFT JOIN marketplace_prices mp ON pb.region = mp.region AND pb.product_id = mp.product_id
    LEFT JOIN scrapped_prices sp ON pb.region = sp.region AND pb.product_id = sp.product_id
    LEFT JOIN yesterday_nmv yn ON pb.cohort_id = yn.cohort_id AND pb.product_id = yn.product_id
),

market_percentiles AS (
    SELECT 
        *,
        LEAST(
            COALESCE(ben_soliman_price, 999999),
            COALESCE(final_min_price, 999999),
            COALESCE(min_scrapped, 999999)
        ) AS market_minimum,
        COALESCE(scrapped25, final_min_price, ben_soliman_price) AS market_25pct,
        COALESCE(scrapped50, final_mod_price, ben_soliman_price) AS market_median,
        COALESCE(scrapped75, final_max_price, ben_soliman_price) AS market_75pct,
        GREATEST(
            COALESCE(ben_soliman_price, 0),
            COALESCE(final_max_price, 0),
            COALESCE(max_scrapped, 0)
        ) AS market_maximum
    FROM market_data_combined
    WHERE ben_soliman_price IS NOT NULL 
        OR final_min_price IS NOT NULL 
        OR min_scrapped IS NOT NULL
),

final_market_position AS (
    SELECT 
        *,
        CASE WHEN current_price > 0 THEN (current_price - wac_p) / current_price ELSE 0 END AS current_margin,
        CASE 
            WHEN current_price IS NULL OR current_price = 0 THEN 'no_price'
            WHEN market_minimum IS NULL OR market_minimum = 999999 THEN 'no_market_data'
            WHEN current_price < market_minimum * 0.98 THEN 'below_market'
            WHEN current_price <= market_minimum * 1.02 THEN 'at_market_min'
            WHEN current_price <= market_25pct * 1.02 THEN 'at_25_percentile'
            WHEN current_price <= market_median * 1.02 THEN 'at_median'
            WHEN current_price <= market_75pct * 1.02 THEN 'at_75_percentile'
            WHEN current_price <= market_maximum * 1.02 THEN 'at_market_max'
            ELSE 'above_market'
        END AS market_position
    FROM market_percentiles
    WHERE market_minimum < 999999
)

-- Weighted summary by yesterday's NMV
SELECT 
    market_position,
    COUNT(*) AS sku_count,
    SUM(yesterday_nmv) AS total_nmv,
    SUM(yesterday_nmv) / NULLIF(SUM(SUM(yesterday_nmv)) OVER(), 0) * 100 AS nmv_pct,
    AVG(current_margin) * 100 AS avg_margin_pct,
    SUM(yesterday_nmv * current_margin) / NULLIF(SUM(yesterday_nmv), 0) * 100 AS weighted_avg_margin_pct
FROM final_market_position
WHERE yesterday_nmv > 0
GROUP BY market_position
ORDER BY 
    CASE market_position
        WHEN 'below_market' THEN 1
        WHEN 'at_market_min' THEN 2
        WHEN 'at_25_percentile' THEN 3
        WHEN 'at_median' THEN 4
        WHEN 'at_75_percentile' THEN 5
        WHEN 'at_market_max' THEN 6
        WHEN 'above_market' THEN 7
        ELSE 8
    END;


-- =============================================================================
-- QUERY 3: MARKET POSITION WEIGHTED BY TODAY'S NMV
-- =============================================================================
-- Returns weighted distribution of market positions based on today's NMV

WITH 
-- Ben Soliman Prices
ben_soliman AS (
    WITH lower AS (
        SELECT DISTINCT product_id, new_d*bs_price AS ben_soliman_price, INJECTION_DATE
        FROM (
            SELECT maxab_product_id AS product_id, INJECTION_DATE, wac1, wac_p,
                (bs_price) AS bs_price, diff, cu_price,
                CASE WHEN p1 > 1 THEN child_quantity ELSE 0 END AS scheck,
                ROUND(p1/2)*2 AS p1, p2,
                CASE WHEN (ROUND(p1 / scheck) * scheck) = 0 THEN p1 ELSE (ROUND(p1 / scheck) * scheck) END AS new_d
            FROM (
                SELECT sm.*, wac1, wac_p, 
                    ABS((bs_price)-(wac_p*maxab_basic_unit_count))/(wac_p*maxab_basic_unit_count) AS diff,
                    cpc.price AS cu_price, pup.child_quantity,
                    ROUND((cu_price/bs_price)) AS p1, 
                    ROUND(((bs_price)/cu_price)) AS p2
                FROM materialized_views.savvy_mapping sm 
                JOIN finance.all_cogs f ON f.product_id = sm.maxab_product_id 
                    AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_Date AND f.to_date
                JOIN PACKING_UNIT_PRODUCTS pu ON pu.product_id = sm.maxab_product_id AND pu.IS_BASIC_UNIT = 1 
                JOIN cohort_product_packing_units cpc ON cpc.PRODUCT_PACKING_UNIT_ID = pu.id AND cohort_id = 700 
                JOIN packing_unit_products pup ON pup.product_id = sm.maxab_product_id AND pup.is_basic_unit = 1  
                WHERE bs_price IS NOT NULL 
                    AND INJECTION_DATE::date >= CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP())::date - 5 
                    AND diff > 0.3 AND p1 > 1
            )
        )
        QUALIFY MAX(INJECTION_DATE) OVER(PARTITION BY product_id) = INJECTION_DATE
    ),
    m_bs AS (
        SELECT z.* FROM (
            SELECT maxab_product_id AS product_id, AVG(bs_final_price) AS ben_soliman_price, INJECTION_DATE
            FROM (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY maxab_product_id ORDER BY diff) AS rnk_2 
                FROM (
                    SELECT *, (bs_final_price-wac_p)/wac_p AS diff_2 
                    FROM (
                        SELECT *, bs_price/maxab_basic_unit_count AS bs_final_price 
                        FROM (
                            SELECT *, ROW_NUMBER() OVER(PARTITION BY maxab_product_id, maxab_pu ORDER BY diff) AS rnk 
                            FROM (
                                SELECT *, MAX(INJECTION_DATE::date) OVER(PARTITION BY maxab_product_id, maxab_pu) AS max_date
                                FROM (
                                    SELECT sm.*, wac1, wac_p, 
                                        ABS(bs_price-(wac_p*maxab_basic_unit_count))/(wac_p*maxab_basic_unit_count) AS diff 
                                    FROM materialized_views.savvy_mapping sm 
                                    JOIN finance.all_cogs f ON f.product_id = sm.maxab_product_id 
                                        AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_Date AND f.to_date
                                    WHERE bs_price IS NOT NULL 
                                        AND INJECTION_DATE::date >= CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP())::date - 5 
                                        AND diff < 0.3
                                )
                                QUALIFY max_date = INJECTION_DATE
                            ) QUALIFY rnk = 1 
                        )
                    ) WHERE diff_2 BETWEEN -0.5 AND 0.5 
                ) QUALIFY rnk_2 = 1 
            ) GROUP BY ALL
        ) z 
        JOIN finance.all_cogs f ON f.product_id = z.product_id 
            AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_Date AND f.to_date
        WHERE ben_soliman_price BETWEEN f.wac_p*0.8 AND f.wac_p*1.3
    )
    SELECT product_id, AVG(ben_soliman_price) AS ben_soliman_price
    FROM (
        SELECT product_id, ben_soliman_price, INJECTION_DATE
        FROM (
            SELECT * FROM (
                SELECT *, 1 AS prio FROM m_bs 
                UNION ALL
                SELECT *, 2 AS prio FROM lower
            )
            QUALIFY MAX(INJECTION_DATE) OVER(PARTITION BY product_id) = INJECTION_DATE
        )
        QUALIFY prio = MIN(prio) OVER(PARTITION BY product_id)
    )
    GROUP BY ALL
),

marketplace_prices AS (
    WITH MP AS (
        SELECT region, product_id,
            MIN(min_price) AS min_price, MIN(max_price) AS max_price,
            MIN(mod_price) AS mod_price, MIN(true_min) AS true_min, MIN(true_max) AS true_max
        FROM (
            SELECT mp.region, mp.product_id, mp.pu_id,
                min_price/BASIC_UNIT_COUNT AS min_price,
                max_price/BASIC_UNIT_COUNT AS max_price,
                mod_price/BASIC_UNIT_COUNT AS mod_price,
                TRUE_MIN_PRICE/BASIC_UNIT_COUNT AS true_min,
                TRUE_MAX_PRICE/BASIC_UNIT_COUNT AS true_max
            FROM materialized_views.marketplace_prices mp 
            JOIN packing_unit_products pup ON pup.product_id = mp.product_id AND pup.packing_unit_id = mp.pu_id
            JOIN finance.all_cogs f ON f.product_id = mp.product_id 
                AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_date AND f.to_date
            WHERE LEAST(min_price, mod_price) BETWEEN wac_p*0.9 AND wac_p*1.3 
        )
        GROUP BY ALL 
    ),
    region_mapping AS (
        SELECT * FROM (VALUES
            ('Delta East', 'Delta West'), ('Delta West', 'Delta East'),
            ('Alexandria', 'Cairo'), ('Alexandria', 'Giza'),
            ('Upper Egypt', 'Cairo'), ('Upper Egypt', 'Giza'),
            ('Cairo', 'Giza'), ('Giza', 'Cairo'),
            ('Delta West', 'Cairo'), ('Delta East', 'Cairo'),
            ('Delta West', 'Giza'), ('Delta East', 'Giza')
        ) AS region_mapping(region, fallback_region)
    ),
    all_regions AS (
        SELECT * FROM (VALUES
            ('Cairo'), ('Giza'), ('Delta West'), ('Delta East'), ('Upper Egypt'), ('Alexandria')
        ) AS x(region)
    ),
    full_data AS (
        SELECT products.id AS product_id, ar.region
        FROM products, all_regions ar
        WHERE activation = 'true'
    )
    SELECT region, product_id,
        MIN(final_min_price) AS final_min_price, 
        MIN(final_max_price) AS final_max_price,
        MIN(final_mod_price) AS final_mod_price
    FROM (
        SELECT DISTINCT w.region, w.product_id,
            COALESCE(m1.min_price, m2.min_price) AS final_min_price,
            COALESCE(m1.max_price, m2.max_price) AS final_max_price,
            COALESCE(m1.mod_price, m2.mod_price) AS final_mod_price
        FROM full_data w
        LEFT JOIN MP m1 ON w.region = m1.region AND w.product_id = m1.product_id
        LEFT JOIN region_mapping rm ON w.region = rm.region
        LEFT JOIN MP m2 ON rm.fallback_region = m2.region AND w.product_id = m2.product_id
    )
    WHERE final_min_price IS NOT NULL 
    GROUP BY ALL
),

scrapped_prices AS (
    SELECT product_id, region,
        MIN(market_price) AS min_scrapped,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY market_price) AS scrapped25,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY market_price) AS scrapped50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY market_price) AS scrapped75,
        MAX(market_price) AS max_scrapped
    FROM (
        SELECT DISTINCT cmp.*, MAX(date) OVER(PARTITION BY region, cmp.product_id, competitor) AS max_date
        FROM MATERIALIZED_VIEWS.CLEANED_MARKET_PRICES cmp
        JOIN finance.all_cogs f ON f.product_id = cmp.product_id 
            AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_date AND f.to_date 
        WHERE date >= CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP())::date - 7 
            AND MARKET_PRICE BETWEEN f.wac_p * 0.8 AND wac_p * 1.3
        QUALIFY date = max_date 
    )
    GROUP BY ALL
),

product_base AS (
    SELECT DISTINCT
        CASE 
            WHEN cohort_id IN (700, 695) THEN 'Cairo'
            WHEN cohort_id IN (701) THEN 'Giza'
            WHEN cohort_id IN (704, 698) THEN 'Delta East'
            WHEN cohort_id IN (703, 697) THEN 'Delta West'
            WHEN cohort_id IN (696, 1123, 1124, 1125, 1126) THEN 'Upper Egypt'
            WHEN cohort_id IN (702, 699) THEN 'Alexandria'
        END AS region,
        cohort_id,
        f.product_id,
        CONCAT(products.name_ar, ' ', products.size, ' ', product_units.name_ar) AS sku_name,
        brands.name_ar AS brand,
        categories.name_ar AS cat,
        f.wac_p
    FROM finance.all_cogs f
    JOIN products ON products.id = f.product_id
    JOIN brands ON products.brand_id = brands.id
    JOIN categories ON products.category_id = categories.id
    JOIN product_units ON product_units.id = products.unit_id
    CROSS JOIN (
        SELECT DISTINCT cohort_id 
        FROM COHORT_PRICING_CHANGES 
        WHERE cohort_id IN (700,701,702,703,704,1123,1124,1125,1126)
    ) cohorts
    WHERE CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_date AND f.to_date
        AND products.activation = 'true'
),

current_prices AS (
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
            cohort_id,
            pu.product_id,
            pu.basic_unit_count,
            AVG(cpu.price) AS price
        FROM cohort_product_packing_units cpu
        JOIN PACKING_UNIT_PRODUCTS pu ON pu.id = cpu.product_packing_unit_id
        WHERE cpu.cohort_id IN (700,701,702,703,704,695,696,697,698,699,1123,1124,1125,1126)
            AND cpu.created_at::date <> '2023-07-31'
            AND cpu.is_customized = TRUE
        GROUP BY ALL
    )
    SELECT region, cohort_id, product_id, price AS current_price
    FROM local_prices
    WHERE basic_unit_count = 1
),

-- Today's NMV
today_nmv AS (
    SELECT 
        cpc.cohort_id, 
        pso.product_id,
        SUM(pso.total_price) AS nmv
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    JOIN COHORT_PRICING_CHANGES cpc ON cpc.id = pso.COHORT_PRICING_CHANGE_id
    WHERE so.created_at::date = CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP())::date
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
        AND cpc.cohort_id IN (700,701,702,703,704,1123,1124,1125,1126)
    GROUP BY ALL
),

market_data_combined AS (
    SELECT 
        pb.region,
        pb.cohort_id,
        pb.product_id,
        pb.sku_name,
        pb.brand,
        pb.cat,
        pb.wac_p,
        cp.current_price,
        bs.ben_soliman_price,
        mp.final_min_price,
        mp.final_max_price,
        mp.final_mod_price,
        sp.min_scrapped,
        sp.scrapped25,
        sp.scrapped50,
        sp.scrapped75,
        sp.max_scrapped,
        COALESCE(tn.nmv, 0) AS today_nmv
    FROM product_base pb
    LEFT JOIN current_prices cp ON pb.cohort_id = cp.cohort_id AND pb.product_id = cp.product_id
    LEFT JOIN ben_soliman bs ON pb.product_id = bs.product_id
    LEFT JOIN marketplace_prices mp ON pb.region = mp.region AND pb.product_id = mp.product_id
    LEFT JOIN scrapped_prices sp ON pb.region = sp.region AND pb.product_id = sp.product_id
    LEFT JOIN today_nmv tn ON pb.cohort_id = tn.cohort_id AND pb.product_id = tn.product_id
),

market_percentiles AS (
    SELECT 
        *,
        LEAST(
            COALESCE(ben_soliman_price, 999999),
            COALESCE(final_min_price, 999999),
            COALESCE(min_scrapped, 999999)
        ) AS market_minimum,
        COALESCE(scrapped25, final_min_price, ben_soliman_price) AS market_25pct,
        COALESCE(scrapped50, final_mod_price, ben_soliman_price) AS market_median,
        COALESCE(scrapped75, final_max_price, ben_soliman_price) AS market_75pct,
        GREATEST(
            COALESCE(ben_soliman_price, 0),
            COALESCE(final_max_price, 0),
            COALESCE(max_scrapped, 0)
        ) AS market_maximum
    FROM market_data_combined
    WHERE ben_soliman_price IS NOT NULL 
        OR final_min_price IS NOT NULL 
        OR min_scrapped IS NOT NULL
),

final_market_position AS (
    SELECT 
        *,
        CASE WHEN current_price > 0 THEN (current_price - wac_p) / current_price ELSE 0 END AS current_margin,
        CASE 
            WHEN current_price IS NULL OR current_price = 0 THEN 'no_price'
            WHEN market_minimum IS NULL OR market_minimum = 999999 THEN 'no_market_data'
            WHEN current_price < market_minimum * 0.98 THEN 'below_market'
            WHEN current_price <= market_minimum * 1.02 THEN 'at_market_min'
            WHEN current_price <= market_25pct * 1.02 THEN 'at_25_percentile'
            WHEN current_price <= market_median * 1.02 THEN 'at_median'
            WHEN current_price <= market_75pct * 1.02 THEN 'at_75_percentile'
            WHEN current_price <= market_maximum * 1.02 THEN 'at_market_max'
            ELSE 'above_market'
        END AS market_position
    FROM market_percentiles
    WHERE market_minimum < 999999
)

-- Weighted summary by today's NMV
SELECT 
    market_position,
    COUNT(*) AS sku_count,
    SUM(today_nmv) AS total_nmv,
    SUM(today_nmv) / NULLIF(SUM(SUM(today_nmv)) OVER(), 0) * 100 AS nmv_pct,
    AVG(current_margin) * 100 AS avg_margin_pct,
    SUM(today_nmv * current_margin) / NULLIF(SUM(today_nmv), 0) * 100 AS weighted_avg_margin_pct
FROM final_market_position
WHERE today_nmv > 0
GROUP BY market_position
ORDER BY 
    CASE market_position
        WHEN 'below_market' THEN 1
        WHEN 'at_market_min' THEN 2
        WHEN 'at_25_percentile' THEN 3
        WHEN 'at_median' THEN 4
        WHEN 'at_75_percentile' THEN 5
        WHEN 'at_market_max' THEN 6
        WHEN 'above_market' THEN 7
        ELSE 8
    END;


-- =============================================================================
-- QUERY 4: DETAILED MARKET POSITION BY REGION (Yesterday's NMV)
-- =============================================================================
-- Breakdown by region with yesterday's NMV weighting

WITH 
ben_soliman AS (
    WITH lower AS (
        SELECT DISTINCT product_id, new_d*bs_price AS ben_soliman_price, INJECTION_DATE
        FROM (
            SELECT maxab_product_id AS product_id, INJECTION_DATE, wac1, wac_p,
                (bs_price) AS bs_price, diff, cu_price,
                CASE WHEN p1 > 1 THEN child_quantity ELSE 0 END AS scheck,
                ROUND(p1/2)*2 AS p1, p2,
                CASE WHEN (ROUND(p1 / scheck) * scheck) = 0 THEN p1 ELSE (ROUND(p1 / scheck) * scheck) END AS new_d
            FROM (
                SELECT sm.*, wac1, wac_p, 
                    ABS((bs_price)-(wac_p*maxab_basic_unit_count))/(wac_p*maxab_basic_unit_count) AS diff,
                    cpc.price AS cu_price, pup.child_quantity,
                    ROUND((cu_price/bs_price)) AS p1, 
                    ROUND(((bs_price)/cu_price)) AS p2
                FROM materialized_views.savvy_mapping sm 
                JOIN finance.all_cogs f ON f.product_id = sm.maxab_product_id 
                    AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_Date AND f.to_date
                JOIN PACKING_UNIT_PRODUCTS pu ON pu.product_id = sm.maxab_product_id AND pu.IS_BASIC_UNIT = 1 
                JOIN cohort_product_packing_units cpc ON cpc.PRODUCT_PACKING_UNIT_ID = pu.id AND cohort_id = 700 
                JOIN packing_unit_products pup ON pup.product_id = sm.maxab_product_id AND pup.is_basic_unit = 1  
                WHERE bs_price IS NOT NULL 
                    AND INJECTION_DATE::date >= CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP())::date - 5 
                    AND diff > 0.3 AND p1 > 1
            )
        )
        QUALIFY MAX(INJECTION_DATE) OVER(PARTITION BY product_id) = INJECTION_DATE
    ),
    m_bs AS (
        SELECT z.* FROM (
            SELECT maxab_product_id AS product_id, AVG(bs_final_price) AS ben_soliman_price, INJECTION_DATE
            FROM (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY maxab_product_id ORDER BY diff) AS rnk_2 
                FROM (
                    SELECT *, (bs_final_price-wac_p)/wac_p AS diff_2 
                    FROM (
                        SELECT *, bs_price/maxab_basic_unit_count AS bs_final_price 
                        FROM (
                            SELECT *, ROW_NUMBER() OVER(PARTITION BY maxab_product_id, maxab_pu ORDER BY diff) AS rnk 
                            FROM (
                                SELECT *, MAX(INJECTION_DATE::date) OVER(PARTITION BY maxab_product_id, maxab_pu) AS max_date
                                FROM (
                                    SELECT sm.*, wac1, wac_p, 
                                        ABS(bs_price-(wac_p*maxab_basic_unit_count))/(wac_p*maxab_basic_unit_count) AS diff 
                                    FROM materialized_views.savvy_mapping sm 
                                    JOIN finance.all_cogs f ON f.product_id = sm.maxab_product_id 
                                        AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_Date AND f.to_date
                                    WHERE bs_price IS NOT NULL 
                                        AND INJECTION_DATE::date >= CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP())::date - 5 
                                        AND diff < 0.3
                                )
                                QUALIFY max_date = INJECTION_DATE
                            ) QUALIFY rnk = 1 
                        )
                    ) WHERE diff_2 BETWEEN -0.5 AND 0.5 
                ) QUALIFY rnk_2 = 1 
            ) GROUP BY ALL
        ) z 
        JOIN finance.all_cogs f ON f.product_id = z.product_id 
            AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_Date AND f.to_date
        WHERE ben_soliman_price BETWEEN f.wac_p*0.8 AND f.wac_p*1.3
    )
    SELECT product_id, AVG(ben_soliman_price) AS ben_soliman_price
    FROM (
        SELECT product_id, ben_soliman_price, INJECTION_DATE
        FROM (
            SELECT * FROM (
                SELECT *, 1 AS prio FROM m_bs 
                UNION ALL
                SELECT *, 2 AS prio FROM lower
            )
            QUALIFY MAX(INJECTION_DATE) OVER(PARTITION BY product_id) = INJECTION_DATE
        )
        QUALIFY prio = MIN(prio) OVER(PARTITION BY product_id)
    )
    GROUP BY ALL
),

marketplace_prices AS (
    WITH MP AS (
        SELECT region, product_id,
            MIN(min_price) AS min_price, MIN(max_price) AS max_price,
            MIN(mod_price) AS mod_price
        FROM (
            SELECT mp.region, mp.product_id, mp.pu_id,
                min_price/BASIC_UNIT_COUNT AS min_price,
                max_price/BASIC_UNIT_COUNT AS max_price,
                mod_price/BASIC_UNIT_COUNT AS mod_price
            FROM materialized_views.marketplace_prices mp 
            JOIN packing_unit_products pup ON pup.product_id = mp.product_id AND pup.packing_unit_id = mp.pu_id
            JOIN finance.all_cogs f ON f.product_id = mp.product_id 
                AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_date AND f.to_date
            WHERE LEAST(min_price, mod_price) BETWEEN wac_p*0.9 AND wac_p*1.3 
        )
        GROUP BY ALL 
    ),
    region_mapping AS (
        SELECT * FROM (VALUES
            ('Delta East', 'Delta West'), ('Delta West', 'Delta East'),
            ('Alexandria', 'Cairo'), ('Alexandria', 'Giza'),
            ('Upper Egypt', 'Cairo'), ('Upper Egypt', 'Giza'),
            ('Cairo', 'Giza'), ('Giza', 'Cairo'),
            ('Delta West', 'Cairo'), ('Delta East', 'Cairo'),
            ('Delta West', 'Giza'), ('Delta East', 'Giza')
        ) AS region_mapping(region, fallback_region)
    ),
    all_regions AS (
        SELECT * FROM (VALUES
            ('Cairo'), ('Giza'), ('Delta West'), ('Delta East'), ('Upper Egypt'), ('Alexandria')
        ) AS x(region)
    ),
    full_data AS (
        SELECT products.id AS product_id, ar.region
        FROM products, all_regions ar
        WHERE activation = 'true'
    )
    SELECT region, product_id,
        MIN(final_min_price) AS final_min_price, 
        MIN(final_max_price) AS final_max_price,
        MIN(final_mod_price) AS final_mod_price
    FROM (
        SELECT DISTINCT w.region, w.product_id,
            COALESCE(m1.min_price, m2.min_price) AS final_min_price,
            COALESCE(m1.max_price, m2.max_price) AS final_max_price,
            COALESCE(m1.mod_price, m2.mod_price) AS final_mod_price
        FROM full_data w
        LEFT JOIN MP m1 ON w.region = m1.region AND w.product_id = m1.product_id
        LEFT JOIN region_mapping rm ON w.region = rm.region
        LEFT JOIN MP m2 ON rm.fallback_region = m2.region AND w.product_id = m2.product_id
    )
    WHERE final_min_price IS NOT NULL 
    GROUP BY ALL
),

scrapped_prices AS (
    SELECT product_id, region,
        MIN(market_price) AS min_scrapped,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY market_price) AS scrapped50,
        MAX(market_price) AS max_scrapped
    FROM (
        SELECT DISTINCT cmp.*, MAX(date) OVER(PARTITION BY region, cmp.product_id, competitor) AS max_date
        FROM MATERIALIZED_VIEWS.CLEANED_MARKET_PRICES cmp
        JOIN finance.all_cogs f ON f.product_id = cmp.product_id 
            AND CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_date AND f.to_date 
        WHERE date >= CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP())::date - 7 
            AND MARKET_PRICE BETWEEN f.wac_p * 0.8 AND wac_p * 1.3
        QUALIFY date = max_date 
    )
    GROUP BY ALL
),

product_base AS (
    SELECT DISTINCT
        CASE 
            WHEN cohort_id IN (700, 695) THEN 'Cairo'
            WHEN cohort_id IN (701) THEN 'Giza'
            WHEN cohort_id IN (704, 698) THEN 'Delta East'
            WHEN cohort_id IN (703, 697) THEN 'Delta West'
            WHEN cohort_id IN (696, 1123, 1124, 1125, 1126) THEN 'Upper Egypt'
            WHEN cohort_id IN (702, 699) THEN 'Alexandria'
        END AS region,
        cohort_id,
        f.product_id,
        f.wac_p
    FROM finance.all_cogs f
    JOIN products ON products.id = f.product_id
    CROSS JOIN (
        SELECT DISTINCT cohort_id 
        FROM COHORT_PRICING_CHANGES 
        WHERE cohort_id IN (700,701,702,703,704,1123,1124,1125,1126)
    ) cohorts
    WHERE CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP()) BETWEEN f.from_date AND f.to_date
        AND products.activation = 'true'
),

current_prices AS (
    SELECT  
        CASE 
            WHEN cpu.cohort_id IN (700, 695) THEN 'Cairo'
            WHEN cpu.cohort_id IN (701) THEN 'Giza'
            WHEN cpu.cohort_id IN (704, 698) THEN 'Delta East'
            WHEN cpu.cohort_id IN (703, 697) THEN 'Delta West'
            WHEN cpu.cohort_id IN (696, 1123, 1124, 1125, 1126) THEN 'Upper Egypt'
            WHEN cpu.cohort_id IN (702, 699) THEN 'Alexandria'
        END AS region,
        cohort_id,
        pu.product_id,
        AVG(cpu.price) AS current_price
    FROM cohort_product_packing_units cpu
    JOIN PACKING_UNIT_PRODUCTS pu ON pu.id = cpu.product_packing_unit_id
    WHERE cpu.cohort_id IN (700,701,702,703,704,695,696,697,698,699,1123,1124,1125,1126)
        AND cpu.created_at::date <> '2023-07-31'
        AND cpu.is_customized = TRUE
        AND pu.basic_unit_count = 1
    GROUP BY ALL
),

yesterday_nmv AS (
    SELECT 
        cpc.cohort_id, 
        pso.product_id,
        SUM(pso.total_price) AS nmv
    FROM product_sales_order pso
    JOIN sales_orders so ON so.id = pso.sales_order_id
    JOIN COHORT_PRICING_CHANGES cpc ON cpc.id = pso.COHORT_PRICING_CHANGE_id
    WHERE so.created_at::date = CONVERT_TIMEZONE('America/Los_Angeles', 'Africa/Cairo', CURRENT_TIMESTAMP())::date - 1
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
        AND cpc.cohort_id IN (700,701,702,703,704,1123,1124,1125,1126)
    GROUP BY ALL
),

market_data_combined AS (
    SELECT 
        pb.region,
        pb.cohort_id,
        pb.product_id,
        pb.wac_p,
        cp.current_price,
        bs.ben_soliman_price,
        mp.final_min_price,
        mp.final_max_price,
        mp.final_mod_price,
        sp.min_scrapped,
        sp.scrapped50,
        sp.max_scrapped,
        COALESCE(yn.nmv, 0) AS yesterday_nmv
    FROM product_base pb
    LEFT JOIN current_prices cp ON pb.cohort_id = cp.cohort_id AND pb.product_id = cp.product_id
    LEFT JOIN ben_soliman bs ON pb.product_id = bs.product_id
    LEFT JOIN marketplace_prices mp ON pb.region = mp.region AND pb.product_id = mp.product_id
    LEFT JOIN scrapped_prices sp ON pb.region = sp.region AND pb.product_id = sp.product_id
    LEFT JOIN yesterday_nmv yn ON pb.cohort_id = yn.cohort_id AND pb.product_id = yn.product_id
),

market_percentiles AS (
    SELECT 
        *,
        LEAST(
            COALESCE(ben_soliman_price, 999999),
            COALESCE(final_min_price, 999999),
            COALESCE(min_scrapped, 999999)
        ) AS market_minimum,
        COALESCE(scrapped50, final_mod_price, ben_soliman_price) AS market_median,
        GREATEST(
            COALESCE(ben_soliman_price, 0),
            COALESCE(final_max_price, 0),
            COALESCE(max_scrapped, 0)
        ) AS market_maximum
    FROM market_data_combined
    WHERE ben_soliman_price IS NOT NULL 
        OR final_min_price IS NOT NULL 
        OR min_scrapped IS NOT NULL
),

final_market_position AS (
    SELECT 
        *,
        CASE WHEN current_price > 0 THEN (current_price - wac_p) / current_price ELSE 0 END AS current_margin,
        CASE 
            WHEN current_price IS NULL OR current_price = 0 THEN 'no_price'
            WHEN market_minimum IS NULL OR market_minimum = 999999 THEN 'no_market_data'
            WHEN current_price < market_minimum * 0.98 THEN 'below_market'
            WHEN current_price <= market_minimum * 1.02 THEN 'at_market_min'
            WHEN current_price <= market_median * 1.02 THEN 'at_median'
            WHEN current_price <= market_maximum * 1.02 THEN 'at_market_max'
            ELSE 'above_market'
        END AS market_position
    FROM market_percentiles
    WHERE market_minimum < 999999
)

-- Regional breakdown with yesterday's NMV
SELECT 
    region,
    market_position,
    COUNT(*) AS sku_count,
    SUM(yesterday_nmv) AS total_nmv,
    SUM(yesterday_nmv) / NULLIF(SUM(SUM(yesterday_nmv)) OVER(PARTITION BY region), 0) * 100 AS region_nmv_pct,
    AVG(current_margin) * 100 AS avg_margin_pct,
    SUM(yesterday_nmv * current_margin) / NULLIF(SUM(yesterday_nmv), 0) * 100 AS weighted_avg_margin_pct
FROM final_market_position
WHERE yesterday_nmv > 0
GROUP BY region, market_position
ORDER BY region,
    CASE market_position
        WHEN 'below_market' THEN 1
        WHEN 'at_market_min' THEN 2
        WHEN 'at_median' THEN 3
        WHEN 'at_market_max' THEN 4
        WHEN 'above_market' THEN 5
        ELSE 6
    END;

