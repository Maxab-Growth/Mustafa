-- =============================================================================
-- MARGIN BOUNDARIES QUERY
-- =============================================================================
-- Computes margin boundaries (min, max, optimal, median) per product per
-- warehouse (pso.warehouse_id).
--
-- Two-track architecture:
--   BOUNDARIES (min/max/median): 1 year of same-quarter data with recency
--       weighting. Captures seasonal patterns without cross-season bleed.
--   OPTIMAL (optimal_bm): Last 120 days, binned GP-maximizing analysis.
--       Finds the margin level that historically generated the highest
--       gross profit (naturally balances volume vs. margin rate).
--
-- Outlier removal: IQR-based spike cleaning shared by both tracks.
-- Seasonality weighting: EXP(-0.023 * days_ago)  ~30-day half-life.
--
-- Replaces: SELECT ... FROM materialized_views.PRODUCT_STATISTICS
-- Usage:    MARGIN_BOUNDARIES_QUERY = f'''<this file contents>'''
--           Use {TIMEZONE} placeholder for Snowflake timezone injection.
-- =============================================================================


-- =========================================================================
-- SHARED BASE: 1 year of daily margin data + IQR outlier removal
-- =========================================================================

WITH all_daily_margins AS (
    SELECT
        so.created_at::DATE AS order_date,
        pso.warehouse_id,
        pso.product_id,
        SUM(pso.total_price) AS nmv,
        SUM(COALESCE(f.wac_p, 0) * pso.purchased_item_count * pso.basic_unit_count) AS cogs,
        DATEDIFF('day', so.created_at::DATE,
            CONVERT_TIMEZONE('{TIMEZONE}', 'Africa/Cairo', CURRENT_TIMESTAMP())::DATE
        ) AS days_ago,
        EXTRACT(QUARTER FROM so.created_at::DATE) AS order_quarter
    FROM product_sales_order pso
    JOIN sales_orders so
        ON so.id = pso.sales_order_id
    JOIN COHORT_PRICING_CHANGES cpc
        ON cpc.id = pso.COHORT_PRICING_CHANGE_id
    JOIN finance.all_cogs f
        ON f.product_id = pso.product_id
        AND f.from_date::DATE <= so.created_at::DATE
        AND f.to_date::DATE  >  so.created_at::DATE
    WHERE so.created_at::DATE BETWEEN
            DATEADD('year', -1,
                CONVERT_TIMEZONE('{TIMEZONE}', 'Africa/Cairo', CURRENT_TIMESTAMP())::DATE)
            AND CONVERT_TIMEZONE('{TIMEZONE}', 'Africa/Cairo', CURRENT_TIMESTAMP())::DATE - 1
        AND so.sales_order_status_id NOT IN (7, 12)
        AND so.channel IN ('telesales', 'retailer')
        AND pso.purchased_item_count <> 0
        AND cpc.cohort_id IN (700, 701, 702, 703, 704, 1123, 1124, 1125, 1126)
    GROUP BY ALL
    HAVING nmv > 0
),

daily_with_margin AS (
    SELECT
        *,
        (nmv - cogs) / NULLIF(nmv, 0) AS daily_margin,
        nmv - cogs AS gross_profit
    FROM all_daily_margins
),

iqr_stats AS (
    SELECT
        product_id,
        warehouse_id,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY daily_margin) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY daily_margin) AS q3
    FROM daily_with_margin
    GROUP BY product_id, warehouse_id
),

iqr_cleaned AS (
    SELECT dm.*
    FROM daily_with_margin dm
    JOIN iqr_stats iq
        ON dm.product_id = iq.product_id
        AND dm.warehouse_id = iq.warehouse_id
    WHERE dm.daily_margin
        BETWEEN iq.q1 - 1.5 * (iq.q3 - iq.q1)
            AND iq.q3 + 1.5 * (iq.q3 - iq.q1)
),


-- =========================================================================
-- BOUNDARIES TRACK: Same quarter from the past year, weighted percentiles
-- =========================================================================

same_quarter_data AS (
    SELECT *
    FROM iqr_cleaned
    WHERE order_quarter = EXTRACT(QUARTER FROM
        CONVERT_TIMEZONE('{TIMEZONE}', 'Africa/Cairo', CURRENT_TIMESTAMP())::DATE)
),

boundary_with_weights AS (
    SELECT
        *,
        EXP(-0.023 * days_ago) AS time_weight
    FROM same_quarter_data
),

boundary_ordered AS (
    SELECT
        product_id,
        warehouse_id,
        daily_margin,
        time_weight,
        SUM(time_weight) OVER (
            PARTITION BY product_id, warehouse_id
            ORDER BY daily_margin
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_weight,
        SUM(time_weight) OVER (
            PARTITION BY product_id, warehouse_id
        ) AS total_weight,
        COUNT(*) OVER (
            PARTITION BY product_id, warehouse_id
        ) AS data_points
    FROM boundary_with_weights
),

quarter_percentiles AS (
    SELECT
        product_id,
        warehouse_id,
        MIN(CASE WHEN cum_weight >= total_weight * 0.10 THEN daily_margin END) AS MIN_BOUNDARY,
        MIN(CASE WHEN cum_weight >= total_weight * 0.50 THEN daily_margin END) AS MEDIAN_BM,
        MIN(CASE WHEN cum_weight >= total_weight * 0.90 THEN daily_margin END) AS MAX_BOUNDARY
    FROM boundary_ordered
    WHERE data_points >= 5
    GROUP BY product_id, warehouse_id
),


-- =========================================================================
-- BOUNDARIES FALLBACK: Full year data for products missing quarter data
-- =========================================================================

full_year_with_weights AS (
    SELECT
        *,
        EXP(-0.023 * days_ago) AS time_weight
    FROM iqr_cleaned
),

full_year_ordered AS (
    SELECT
        product_id,
        warehouse_id,
        daily_margin,
        time_weight,
        SUM(time_weight) OVER (
            PARTITION BY product_id, warehouse_id
            ORDER BY daily_margin
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_weight,
        SUM(time_weight) OVER (
            PARTITION BY product_id, warehouse_id
        ) AS total_weight,
        COUNT(*) OVER (
            PARTITION BY product_id, warehouse_id
        ) AS data_points
    FROM full_year_with_weights
),

full_year_percentiles AS (
    SELECT
        product_id,
        warehouse_id,
        MIN(CASE WHEN cum_weight >= total_weight * 0.10 THEN daily_margin END) AS MIN_BOUNDARY,
        MIN(CASE WHEN cum_weight >= total_weight * 0.50 THEN daily_margin END) AS MEDIAN_BM,
        MIN(CASE WHEN cum_weight >= total_weight * 0.98 THEN daily_margin END) AS MAX_BOUNDARY
    FROM full_year_ordered
    WHERE data_points >= 5
    GROUP BY product_id, warehouse_id
),

weighted_percentiles AS (
    SELECT
        COALESCE(qp.product_id,   fy.product_id)   AS product_id,
        COALESCE(qp.warehouse_id, fy.warehouse_id) AS warehouse_id,
        COALESCE(qp.MIN_BOUNDARY, fy.MIN_BOUNDARY) AS MIN_BOUNDARY,
        COALESCE(qp.MEDIAN_BM,    fy.MEDIAN_BM)    AS MEDIAN_BM,
        COALESCE(qp.MAX_BOUNDARY, fy.MAX_BOUNDARY) AS MAX_BOUNDARY
    FROM full_year_percentiles fy
    LEFT JOIN quarter_percentiles qp
        ON fy.product_id = qp.product_id
        AND fy.warehouse_id = qp.warehouse_id
),


-- =========================================================================
-- OPTIMAL TRACK: Last 120 days, GP-maximizing margin bin
-- =========================================================================

recent_data AS (
    SELECT
        *,
        EXP(-0.023 * days_ago) AS time_weight
    FROM iqr_cleaned
    WHERE days_ago <= 120
),

margin_bins AS (
    SELECT
        product_id,
        warehouse_id,
        ROUND(daily_margin, 2) AS margin_bin,
        SUM(nmv * time_weight)          AS weighted_nmv,
        SUM(gross_profit * time_weight) AS weighted_gp,
        COUNT(*)                        AS obs_count
    FROM recent_data
    GROUP BY product_id, warehouse_id, ROUND(daily_margin, 2)
),

smoothed_performance AS (
    SELECT
        product_id,
        warehouse_id,
        margin_bin,
        obs_count,
        AVG(weighted_nmv) OVER (
            PARTITION BY product_id, warehouse_id
            ORDER BY margin_bin
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        ) AS smooth_nmv,
        AVG(weighted_gp) OVER (
            PARTITION BY product_id, warehouse_id
            ORDER BY margin_bin
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        ) AS smooth_gp,
        SUM(obs_count) OVER (
            PARTITION BY product_id, warehouse_id
            ORDER BY margin_bin
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        ) AS smooth_obs
    FROM margin_bins
),

optimal_margin AS (
    SELECT
        product_id,
        warehouse_id,
        margin_bin AS optimal_bm
    FROM smoothed_performance
    WHERE smooth_obs >= 2
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY product_id, warehouse_id
        ORDER BY smooth_gp DESC, smooth_nmv DESC
    ) = 1
)


-- =========================================================================
-- FINAL OUTPUT
-- =========================================================================

SELECT
    COALESCE(wp.product_id, om.product_id) AS product_id,
    COALESCE(wp.warehouse_id, om.warehouse_id) AS warehouse_id,
    om.optimal_bm,
    wp.MIN_BOUNDARY,
    wp.MAX_BOUNDARY,
    wp.MEDIAN_BM
FROM weighted_percentiles wp
FULL OUTER JOIN optimal_margin om
    ON wp.product_id = om.product_id
    AND wp.warehouse_id = om.warehouse_id
WHERE COALESCE(wp.MIN_BOUNDARY, om.optimal_bm) IS NOT NULL
