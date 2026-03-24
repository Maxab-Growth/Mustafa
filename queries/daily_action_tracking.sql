-- =============================================================================
-- DAILY ACTION TRACKING QUERY
-- =============================================================================
-- This query provides a complete audit trail of all pricing actions taken 
-- throughout the day per SKU-warehouse, showing:
-- - Base information: date, SKU, warehouse, stocks, yesterday_status
-- - Module 2 (Initial Push) actions at 8 AM
-- - Module 3 (Periodic) actions at 12 PM, 5 PM, 11 PM
-- - Module 4 (Hourly) actions at 9 AM, 10 AM, 11 AM, 1 PM, 2 PM, 3 PM, 4 PM, 6 PM, 7 PM, 8 PM, 9 PM, 10 PM
-- - Combined action and reason for each hour
-- - Price tracking: current_price, new_price, price_change, price_change_pct
--   (if new_price is null, it equals current_price)
-- - Cart rule tracking: current_cart_rule, new_cart_rule, cart_rule_change, cart_rule_change_pct
--   (if new_cart_rule is null, it equals current_cart_rule)
-- - Quantity change tracking (UTH qty changes between actions)
-- =============================================================================

WITH 
-- Step 1: Get Base Data from Module 2 (Initial Push - 8 AM)
module2_base AS (
    SELECT 
        product_id,
        warehouse_id,
        cohort_id,
        sku,
        brand,
        cat,
        stocks,
        yesterday_status,
        price_action,
        price_reason,
        current_price,
        new_price,
        current_cart_rule,
        new_cart_rule,
        8 AS action_hour,  -- Module 2 runs at 8 AM, created_at is DATE not TIMESTAMP
        created_at::date AS action_date
    FROM MATERIALIZED_VIEWS.pricing_initial_push
    WHERE created_at::date = CURRENT_DATE - 1
),

-- Step 2: Get Module 3 Actions (Periodic - 12 PM, 5 PM, 11 PM)
module3_actions AS (
    SELECT 
        product_id,
        warehouse_id,
        -- Combine actions: price + SKU discount + QD
        CONCAT_WS(', ',
            CASE WHEN price_action IS NOT NULL 
                     AND UPPER(TRIM(CAST(price_action AS VARCHAR))) NOT IN ('NONE', 'HOLD', '')
                 THEN CONCAT('Price: ', CAST(price_action AS VARCHAR)) ELSE NULL END,
            CASE WHEN activate_sku_discount IS NOT NULL 
                     AND UPPER(TRIM(CAST(activate_sku_discount AS VARCHAR))) != 'NONE'
                 THEN CONCAT('SKU: ', CAST(activate_sku_discount AS VARCHAR)) ELSE NULL END,
            CASE WHEN activate_qd IS NOT NULL 
                     AND UPPER(TRIM(CAST(activate_qd AS VARCHAR))) != 'NONE'
                 THEN CONCAT('QD: ', CAST(activate_qd AS VARCHAR)) ELSE NULL END
        ) AS combined_action,
        -- Get action reason
        COALESCE(action_reason, 'No reason provided') AS combined_reason,
        current_price,
        new_price,
        current_cart_rule,
        new_cart_rule,
        uth_qty,
        EXTRACT(HOUR FROM created_at::timestamp) AS action_hour,
        created_at::date AS action_date
    FROM MATERIALIZED_VIEWS.pricing_periodic_push
    WHERE created_at::date = CURRENT_DATE - 1
        AND EXTRACT(HOUR FROM created_at::timestamp) IN (12, 17, 23)  -- 12 PM, 5 PM, 11 PM
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY product_id, warehouse_id, EXTRACT(HOUR FROM created_at::timestamp) 
        ORDER BY created_at DESC
    ) = 1
),

-- Step 3: Get Module 4 Actions (Hourly - 9 AM, 10 AM, 11 AM, 1 PM, 2 PM, 3 PM, 4 PM, 6 PM, 7 PM, 8 PM, 9 PM, 10 PM)
module4_actions AS (
    SELECT 
        product_id,
        warehouse_id,
        -- Combine actions: price + cart rule
        CONCAT_WS(', ',
            CASE WHEN price_action IS NOT NULL 
                     AND UPPER(TRIM(CAST(price_action AS VARCHAR))) NOT IN ('NONE', 'HOLD', '')
                 THEN CONCAT('Price: ', CAST(price_action AS VARCHAR)) ELSE NULL END,
            CASE WHEN cart_rule_action IS NOT NULL 
                     AND UPPER(TRIM(CAST(cart_rule_action AS VARCHAR))) != 'NONE'
                 THEN CONCAT('Cart: ', CAST(cart_rule_action AS VARCHAR)) ELSE NULL END
        ) AS combined_action,
        -- Get action reason (construct from status if not available)
        COALESCE(
            action_reason,
            CONCAT_WS('; ',
                CASE WHEN uth_qty_status IS NOT NULL THEN CONCAT('UTH: ', uth_qty_status) ELSE NULL END,
                CASE WHEN last_hour_qty_status IS NOT NULL THEN CONCAT('Last Hour: ', last_hour_qty_status) ELSE NULL END
            ),
            'No reason provided'
        ) AS combined_reason,
        current_price,
        new_price,
        current_cart_rule,
        new_cart_rule,
        -- Module 4 might not have uth_qty in the table, use NULL if not available
        NULL AS uth_qty,  -- Will be calculated from sales data if needed
        EXTRACT(HOUR FROM created_at::timestamp) AS action_hour,
        created_at::date AS action_date
    FROM MATERIALIZED_VIEWS.pricing_hourly_push
    WHERE created_at::date = CURRENT_DATE - 1
        AND EXTRACT(HOUR FROM created_at::timestamp) IN (9, 10, 11, 13, 14, 15, 16, 18, 19, 20, 21, 22)  -- 9 AM, 10 AM, 11 AM, 1 PM, 2 PM, 3 PM, 4 PM, 6 PM, 7 PM, 8 PM, 9 PM, 10 PM
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY product_id, warehouse_id, EXTRACT(HOUR FROM created_at::timestamp) 
        ORDER BY created_at DESC
    ) = 1
),

-- Step 4: Prepare Module 2 actions in same format
module2_actions AS (
    SELECT 
        product_id,
        warehouse_id,
        -- Module 2 only has price_action (no separate SKU/QD/cart actions)
        CASE WHEN price_action IS NOT NULL 
                 AND UPPER(TRIM(CAST(price_action AS VARCHAR))) NOT IN ('NONE', 'HOLD', '')
             THEN CONCAT('Price: ', CAST(price_action AS VARCHAR)) 
             ELSE NULL END AS combined_action,
        COALESCE(price_reason, 'No reason provided') AS combined_reason,
        current_price,
        new_price,
        current_cart_rule,
        new_cart_rule,
        NULL AS uth_qty,  -- Module 2 doesn't have UTH qty
        action_hour,
        action_date
    FROM module2_base
),

-- Step 5: Combine All Actions by Hour
all_actions AS (
    SELECT 
        product_id,
        warehouse_id,
        action_hour,
        action_date,
        combined_action,
        combined_reason,
        current_price,
        new_price,
        current_cart_rule,
        new_cart_rule,
        uth_qty
    FROM module2_actions
    
    UNION ALL
    
    SELECT 
        product_id,
        warehouse_id,
        action_hour,
        action_date,
        combined_action,
        combined_reason,
        current_price,
        new_price,
        current_cart_rule,
        new_cart_rule,
        uth_qty
    FROM module3_actions
    
    UNION ALL
    
    SELECT 
        product_id,
        warehouse_id,
        action_hour,
        action_date,
        combined_action,
        combined_reason,
        current_price,
        new_price,
        current_cart_rule,
        new_cart_rule,
        uth_qty
    FROM module4_actions
),

-- Step 6: Calculate Quantity Changes and Price/Cart Rule Changes
-- First, create a proper hour ordering based on scheduler: 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23
actions_with_hour_order AS (
    SELECT 
        product_id,
        warehouse_id,
        action_hour,
        action_date,
        combined_action,
        combined_reason,
        current_price,
        new_price,
        current_cart_rule,
        new_cart_rule,
        uth_qty,
        -- Create hour order based on scheduler
        CASE action_hour
            WHEN 8 THEN 1   -- Module 2
            WHEN 9 THEN 2    -- Module 4
            WHEN 10 THEN 3   -- Module 4
            WHEN 11 THEN 4   -- Module 4
            WHEN 12 THEN 5   -- Module 3
            WHEN 13 THEN 6   -- Module 4
            WHEN 14 THEN 7   -- Module 4
            WHEN 15 THEN 8   -- Module 4
            WHEN 16 THEN 9   -- Module 4
            WHEN 17 THEN 10  -- Module 3
            WHEN 18 THEN 11  -- Module 4
            WHEN 19 THEN 12  -- Module 4
            WHEN 20 THEN 13  -- Module 4
            WHEN 21 THEN 14  -- Module 4
            WHEN 22 THEN 15  -- Module 4
            WHEN 23 THEN 16  -- Module 3
            ELSE 99
        END AS hour_order
    FROM all_actions
),
actions_with_changes AS (
    SELECT 
        product_id,
        warehouse_id,
        action_hour,
        action_date,
        combined_action,
        combined_reason,
        current_price,
        -- If new_price is null, it equals current_price
        COALESCE(new_price, current_price) AS new_price,
        current_cart_rule,
        -- If new_cart_rule is null, it equals current_cart_rule
        COALESCE(new_cart_rule, current_cart_rule) AS new_cart_rule,
        uth_qty,
        -- Calculate price change (using original columns before COALESCE)
        COALESCE(new_price, current_price) - current_price AS price_change,
        -- Calculate price change percentage
        CASE 
            WHEN current_price > 0 
            THEN ((COALESCE(new_price, current_price) - current_price) / current_price) * 100
            ELSE NULL
        END AS price_change_pct,
        -- Calculate cart rule change (using original columns before COALESCE)
        COALESCE(new_cart_rule, current_cart_rule) - current_cart_rule AS cart_rule_change,
        -- Calculate cart rule change percentage
        CASE 
            WHEN current_cart_rule > 0 AND current_cart_rule != 999
            THEN ((COALESCE(new_cart_rule, current_cart_rule) - current_cart_rule) / current_cart_rule) * 100
            ELSE NULL
        END AS cart_rule_change_pct,
        -- Calculate qty change from previous hour (using hour_order for proper sequence)
        uth_qty - LAG(uth_qty) OVER (
            PARTITION BY product_id, warehouse_id, action_date 
            ORDER BY hour_order
        ) AS qty_change,
        -- Calculate qty change percentage
        CASE 
            WHEN LAG(uth_qty) OVER (
                PARTITION BY product_id, warehouse_id, action_date 
                ORDER BY hour_order
            ) > 0
            THEN ((uth_qty - LAG(uth_qty) OVER (
                PARTITION BY product_id, warehouse_id, action_date 
                ORDER BY hour_order
            )) / LAG(uth_qty) OVER (
                PARTITION BY product_id, warehouse_id, action_date 
                ORDER BY hour_order
            )) * 100
            ELSE NULL
        END AS qty_change_pct
    FROM actions_with_hour_order
),

-- Step 7: Filter to only hours with actions (not empty)
actions_with_data AS (
    SELECT 
        product_id,
        warehouse_id,
        action_hour,
        action_date,
        combined_action,
        combined_reason,
        current_price,
        new_price,
        price_change,
        price_change_pct,
        current_cart_rule,
        new_cart_rule,
        cart_rule_change,
        cart_rule_change_pct,
        uth_qty,
        qty_change,
        qty_change_pct
    FROM actions_with_changes
    WHERE combined_action IS NOT NULL 
        AND TRIM(combined_action) != ''
)

-- Step 8: Join with Base Data and Output One Row Per Hour
SELECT 
    m2.action_date AS date,
    m2.product_id,
    m2.warehouse_id,
    m2.cohort_id,
    m2.sku,
    m2.brand,
    m2.cat,
    m2.stocks,
    m2.yesterday_status,
    a.action_hour,
    CASE a.action_hour
        WHEN 8 THEN 'Module 2 - 8 AM'
        WHEN 9 THEN 'Module 4 - 9 AM'
        WHEN 10 THEN 'Module 4 - 10 AM'
        WHEN 11 THEN 'Module 4 - 11 AM'
        WHEN 12 THEN 'Module 3 - 12 PM'
        WHEN 13 THEN 'Module 4 - 1 PM'
        WHEN 14 THEN 'Module 4 - 2 PM'
        WHEN 15 THEN 'Module 4 - 3 PM'
        WHEN 16 THEN 'Module 4 - 4 PM'
        WHEN 17 THEN 'Module 3 - 5 PM'
        WHEN 18 THEN 'Module 4 - 6 PM'
        WHEN 19 THEN 'Module 4 - 7 PM'
        WHEN 20 THEN 'Module 4 - 8 PM'
        WHEN 21 THEN 'Module 4 - 9 PM'
        WHEN 22 THEN 'Module 4 - 10 PM'
        WHEN 23 THEN 'Module 3 - 11 PM'
        ELSE 'Unknown'
    END AS hour_description,
    a.combined_action AS action,
    a.combined_reason AS reason,
    a.current_price,
    a.new_price,
    a.price_change,
    a.price_change_pct,
    a.current_cart_rule,
    a.new_cart_rule,
    a.cart_rule_change,
    a.cart_rule_change_pct,
    a.uth_qty,
    a.qty_change,
    a.qty_change_pct
FROM module2_base m2
INNER JOIN actions_with_data a
    ON m2.product_id = a.product_id 
    AND m2.warehouse_id = a.warehouse_id
    AND m2.action_date = a.action_date	
ORDER BY m2.stocks desc,action_hour 

