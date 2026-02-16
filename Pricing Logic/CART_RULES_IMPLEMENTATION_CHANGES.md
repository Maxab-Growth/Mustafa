# Cart Rules Implementation Changes Summary

## Module 2: Initial Price Push

### Changes Made:
1. ✅ Updated constants: MIN_CART_RULE=10, MAX_CART_RULE=500
2. ✅ Added percentile data loading query
3. ✅ Replaced get_initial_cart_rule() with percentile-based logic
4. ✅ Removed CART_MULTIPLIERS and get_cart_multiplier()
5. ✅ Updated all calls to get_initial_cart_rule() to pass percentile_data
6. ✅ Special cases: OOS=95%, Zero Demand=95%, Low Stock=50%

## Module 3: Periodic Actions

### Changes Needed:
1. Update MAX_CART_RULE from 150 to 500
2. Add percentile data loading (same query as Module 2)
3. Add helper functions:
   - get_current_percentile_level(current_cart_rule, percentile_row)
   - get_next_lower_percentile(current_level, percentile_row)
4. Modify Growing status logic (around line 1803-1805):
   - Change from: Always reduce cart rule
   - Change to: Only reduce if qty_ratio > retailer_ratio * 1.20
   - Use percentile-based reduction instead of adjust_cart_rule()
5. Ensure cart rules are rounded

## Module 4: Hourly Updates

### Changes Needed:
1. Update MAX_CART_RULE from 150 to 500
2. Add percentile data loading (same query)
3. Add same helper functions as Module 3
4. Modify get_qty_growing_cart_rule():
   - Change from: Min of 3 options
   - Change to: Only reduce if qty_ratio > 1.5, use percentile-based reduction
   - Keep current cart rule otherwise
5. Ensure cart rules are rounded
6. Don't add new columns to output dataframe

## Percentile Query (All Modules)
```sql
select cohort_id, product_id, sku, brand, cat,
       PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY purchased_item_count) as perc_25,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY purchased_item_count) as perc_50,
       PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY purchased_item_count) as perc_75,
       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY purchased_item_count) as perc_95
from (
    select *, count(distinct order_id) over (partition by retailer_id, product_id) as rets_order
    from (
        select cpc.cohort_id, pso.product_id,
               CONCAT(p.name_ar,' ',p.size,' ',product_units.name_ar) as sku,
               b.name_ar as brand, c.name_ar as cat,
               so.id as order_id, so.retailer_id,
               purchased_item_count * pso.basic_unit_count as purchased_item_count,
               pso.total_price as nmv
        from sales_orders so  
        join product_sales_order pso on pso.sales_order_id = so.id
        join COHORT_PRICING_CHANGES cpc on cpc.id = pso.COHORT_PRICING_CHANGE_ID
        join products p on p.id = pso.product_id 
        join categories c on c.id = p.category_id
        join brands b on b.id = p.brand_id
        JOIN product_units ON product_units.id = p.unit_id 
        join PACKING_UNIT_PRODUCTS pup on pso.product_id = pup.product_id and pso.packing_unit_id = pup.packing_unit_id
        where so.created_at::Date >= date_trunc('month', current_date - 120)
        and sales_order_status_id not in (7,12)
        and cpc.cohort_id in (700,701,702,703,704,1123,1124,1125,1126)
        and pup.is_basic_unit = 1 
    )
    qualify rets_order >= 2 
)
group by all
```

## Helper Functions Needed (Modules 3 & 4)

```python
def get_current_percentile_level(current_cart_rule, percentile_row):
    """Determine which percentile level current cart rule corresponds to."""
    if len(percentile_row) == 0:
        return None
    
    perc_95 = percentile_row.iloc[0]['perc_95']
    perc_75 = percentile_row.iloc[0]['perc_75']
    perc_50 = percentile_row.iloc[0]['perc_50']
    perc_25 = percentile_row.iloc[0]['perc_25']
    
    # Determine current level (with tolerance for rounding)
    if pd.notna(perc_95) and abs(current_cart_rule - perc_95) <= 2:
        return 95
    elif pd.notna(perc_75) and abs(current_cart_rule - perc_75) <= 2:
        return 75
    elif pd.notna(perc_50) and abs(current_cart_rule - perc_50) <= 2:
        return 50
    elif pd.notna(perc_25) and abs(current_cart_rule - perc_25) <= 2:
        return 25
    return None

def get_next_lower_percentile(current_level, percentile_row):
    """Get next lower percentile value."""
    if len(percentile_row) == 0:
        return None
    
    if current_level == 95:
        return percentile_row.iloc[0]['perc_75']
    elif current_level == 75:
        return percentile_row.iloc[0]['perc_50']
    elif current_level == 50:
        return percentile_row.iloc[0]['perc_25']
    elif current_level == 25:
        return percentile_row.iloc[0]['perc_25']  # Stay at minimum
    return None
```

