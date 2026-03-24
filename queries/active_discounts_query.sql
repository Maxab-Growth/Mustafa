-- =============================================================================
-- ACTIVE DISCOUNTS QUERY: Currently Active SKU Discounts and QDs
-- =============================================================================
-- Query to get all currently active discounts for Module 3 decisions
-- Parameters:
--   {timezone} - Timezone for conversion
--   {warehouse_ids} - Comma-separated warehouse IDs
-- =============================================================================

-- =============================================================================
-- PART 1: ACTIVE SKU DISCOUNTS
-- =============================================================================
-- NOTE: Adjust table/column names based on your actual schema

WITH active_sku_discounts AS (
    SELECT
        pd.product_id,
        pd.warehouse_id,
        pd.id AS discount_id,
        pd.discount_value AS discount_percentage,
        pd.start_date,
        pd.end_date,
        'active' AS status
    FROM product_discounts pd  -- TODO: Replace with actual table name
    WHERE pd.is_active = TRUE
        AND pd.warehouse_id IN ({warehouse_ids})
        AND (pd.end_date IS NULL OR pd.end_date >= CONVERT_TIMEZONE('{timezone}', 'Africa/Cairo', CURRENT_TIMESTAMP())::DATE)
        AND pd.start_date <= CONVERT_TIMEZONE('{timezone}', 'Africa/Cairo', CURRENT_TIMESTAMP())::DATE
)

SELECT * FROM active_sku_discounts;


-- =============================================================================
-- PART 2: ACTIVE QUANTITY DISCOUNTS
-- =============================================================================

WITH active_qd AS (
    SELECT
        qd.product_id,
        qd.warehouse_id,
        qd.id AS qd_id,
        qd.tier1_qty,
        qd.tier1_discount,
        qd.tier2_qty,
        qd.tier2_discount,
        qd.tier3_qty,
        qd.tier3_discount,
        qd.start_date,
        qd.end_date,
        'active' AS status
    FROM quantity_discounts qd  -- TODO: Replace with actual table name
    WHERE qd.is_active = TRUE
        AND qd.warehouse_id IN ({warehouse_ids})
        AND (qd.end_date IS NULL OR qd.end_date >= CONVERT_TIMEZONE('{timezone}', 'Africa/Cairo', CURRENT_TIMESTAMP())::DATE)
        AND qd.start_date <= CONVERT_TIMEZONE('{timezone}', 'Africa/Cairo', CURRENT_TIMESTAMP())::DATE
)

SELECT * FROM active_qd;

