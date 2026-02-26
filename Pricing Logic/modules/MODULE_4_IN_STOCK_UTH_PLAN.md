# Plan: Add in-stock hours contribution to UTH targets in Module 4

Apply the same distribution-weighted in-stock hours logic used in Module 3 to module_4_hourly_updates.ipynb, so UTH targets and std are based on in-stock contribution and never exceed the cumulative UTH %.

---

## Current Module 4 behavior

- **Data:** Loads get_hourly_distribution() into df_hourly_dist (avg_uth_pct_qty, avg_uth_pct_retailers, avg_last_hour_pct_*). Merges on (warehouse_id, cat). No stock snapshots or per-hour curve.
- **UTH targets (vectorized):**
  - uth_qty_target = p80_daily_240d * avg_uth_pct_qty
  - uth_rets_target = p70_daily_retailers_240d * avg_uth_pct_retailers
  - uth_qty_std = std_daily_240d * avg_uth_pct_qty
  - uth_rets_std = std_daily_retailers_240d * avg_uth_pct_retailers
- **CURRENT_HOUR** is already defined (CAIRO_NOW.hour).
- **Last-hour** targets stay as they are; only UTH targets use in-stock contribution.

---

## Step 1: Load extra data

In the same cell as get_hourly_distribution() (or the next cell):

- Call **get_hourly_contribution_by_hour()** from queries_module and store result in **df_hourly_curve** (columns: warehouse_id, cat, hour, pct_contribution_qty, pct_contribution_retailers).
- Call **get_stock_snapshots_today()** from queries_module and store result in **df_stock_snapshots** (columns: product_id, warehouse_id, hour, available_stock).

No changes to queries_module; both functions already exist.

---

## Step 2: Compute in-stock contribution after merging hourly distribution

**Where:** In the merge cell, after merging df_hourly_dist into df and filling NaNs for avg_uth_pct_qty and avg_uth_pct_retailers, and before the cell that calculates targets.

**What to do (same logic as Module 3):**

**2a. Curve cumulative**

- From df_hourly_curve, keep rows with hour < CURRENT_HOUR.
- Group by (warehouse_id, cat) and sum: curve_cumulative_qty = sum of pct_contribution_qty, curve_cumulative_ret = sum of pct_contribution_retailers.
- Merge this into df on (warehouse_id, cat). If the cumulative dataframe is empty, add columns curve_cumulative_qty and curve_cumulative_ret with NaN.

**2b. In-stock hours**

- From df_stock_snapshots, keep rows where available_stock > 0 and hour < CURRENT_HOUR.
- Keep only (product_id, warehouse_id, hour). Drop duplicates.

**2c. Raw in-stock contribution**

- Merge in-stock hours with df[['product_id', 'warehouse_id', 'cat']].drop_duplicates() on (product_id, warehouse_id) to get cat.
- Merge with df_hourly_curve on (warehouse_id, cat, hour).
- Group by (product_id, warehouse_id) and sum: in_stock_raw_qty, in_stock_raw_ret.
- Merge this result into df on (product_id, warehouse_id). If there are no in-stock rows, create in_stock_raw_qty and in_stock_raw_ret as NaN.

**2d. Scale so result cannot exceed UTH %**

- in_stock_contribution_qty = (in_stock_raw_qty / curve_cumulative_qty).clip(upper=1.0) * avg_uth_pct_qty when curve_cumulative_qty is not NA and > 0; else avg_uth_pct_qty.
- in_stock_contribution_ret = same formula using curve_cumulative_ret, in_stock_raw_ret, avg_uth_pct_retailers.

**2e. Edge cases**

- Fill NaN in_stock_contribution_qty and in_stock_contribution_ret with avg_uth_pct_qty and avg_uth_pct_retailers.
- Where in_stock_contribution_qty or in_stock_contribution_ret is <= 0, set to avg_uth_pct_qty / avg_uth_pct_retailers.

**2f. Cleanup**

- Drop temporary columns: curve_cumulative_qty, curve_cumulative_ret, in_stock_raw_qty, in_stock_raw_ret.
- Do not add in_stock_contribution_qty or in_stock_contribution_ret to any final output column list.

---

## Step 3: Use in-stock contribution for UTH targets and std

**Where:** The cell that currently sets uth_qty_target, uth_rets_target, uth_qty_std, uth_rets_std.

**Replace:**

- uth_qty_target = p80_daily_240d * avg_uth_pct_qty  
  **with** uth_qty_target = p80_daily_240d * in_stock_contribution_qty

- uth_rets_target = p70_daily_retailers_240d * avg_uth_pct_retailers  
  **with** uth_rets_target = p70_daily_retailers_240d * in_stock_contribution_ret

- uth_qty_std = std_daily_240d * avg_uth_pct_qty  
  **with** uth_qty_std = std_daily_240d * in_stock_contribution_qty

- uth_rets_std = std_daily_retailers_240d * avg_uth_pct_retailers  
  **with** uth_rets_std = std_daily_retailers_240d * in_stock_contribution_ret

Do not change last-hour target or std formulas (they keep using avg_last_hour_pct_*).

---

## Step 4: Output and other logic

- Do not add in_stock_contribution_qty or in_stock_contribution_ret to any exported columns; final table schema stays the same.
- No change to last-hour logic, get_status_std, or condition flags; they keep using uth_qty_target and uth_rets_target, which will now use the new in-stock contribution.

---

## Summary checklist

- **Step 1:** In the cell that loads df_hourly_dist, also load df_hourly_curve (get_hourly_contribution_by_hour) and df_stock_snapshots (get_stock_snapshots_today).
- **Step 2:** In the merge cell, after df_hourly_dist merge and fillna, add the in-stock contribution block: curve cumulative, in-stock hours, raw sum, scale by (raw/cumulative)*avg_uth_pct, edge cases, drop temp columns.
- **Step 3:** In the targets cell, set uth_qty_target, uth_rets_target, uth_qty_std, uth_rets_std using in_stock_contribution_qty and in_stock_contribution_ret instead of avg_uth_pct_qty and avg_uth_pct_retailers.
- **Step 4:** Ensure in_stock_contribution_* are not added to any final output column list.

---

## Reference

- Module 3: in-stock contribution is computed in the merge section after the hourly distribution merge; generate_periodic_action uses in_stock_contribution_qty and in_stock_contribution_ret for uth_qty_target and uth_retailer_target.
- Queries: get_hourly_contribution_by_hour() and get_stock_snapshots_today() in queries_module.ipynb.
