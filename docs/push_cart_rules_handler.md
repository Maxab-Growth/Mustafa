# Push Cart Rules Handler

## Purpose

Low-level MaxAB API helper for uploading per-SKU cart rules (`MAX_PER_SALES_ORDER`) to the customized-cart endpoint per cohort. Cart rules cap how many units of a SKU a single retailer can buy in one order — used by M2 and M3 to throttle high-DOH SKUs and protect low-stock SKUs.

Lives at `Mustafa/modules/push_cart_rules_handler.ipynb`. Loaded via `%run push_cart_rules_handler.ipynb` by M2 and M3.

---

## Public surface

| Function | Description |
|---|---|
| `push_cart_rules(df, source_module, mode='testing')` | High-level entry: takes a (cohort_id, product_id, packing_unit_id, max_per_sales_order) DataFrame, builds per-cohort Excels with auto-mirror to main cohorts, calls `post_cart_rules()` per chunk, returns a per-cohort summary. |
| `post_cart_rules(file_path, cohort_id)` | Low-level: POSTs a single Excel file to the MaxAB customized-cart endpoint for a given cohort. |
| `get_access_token()` | Refreshes and caches the API access token. (Same token store as `push_prices_handler` — both share the auth path.) |

---

## Cohort mirroring

Same pattern as `push_prices_handler`:

| Custom cohort | Mirrors to |
|---|---|
| 700 (Cairo) | 695 |
| 701 (Giza) | 61 |
| 702 (Alexandria) | 699 |
| 703 (Delta West) | 697 |
| 704 (Delta East) | 698 |
| 1123-1126 (Upper Egypt) | 696 |

---

## Excel template

| Column | Source |
|---|---|
| Product ID | `df['product_id']` |
| Packing Unit ID | `df['packing_unit_id']` |
| Cohort | Target cohort_id |
| MAX_PER_SALES_ORDER | `df['max_per_sales_order']` (capped within `[MIN_CART_RULE, MAX_CART_RULE]` by caller) |
| Execute At | Cairo timestamp |

---

## Mode

Same as `push_prices_handler`:
- `testing` — builds Excel files but does not upload.
- `live` — uploads each chunk.

---

## Push order convention

When both prices and cart rules change for the same SKU, callers (M2, M3) must push **cart rules first, then prices**. The reason: a price change without a corresponding cart rule update can momentarily expose a SKU at the new price with the old (looser) cart rule, which can drain inventory faster than intended. Pushing cart rules first ensures the throttle is in place before the price moves.

---

## Dependencies

| Direction | Module |
|---|---|
| **Called by** | `module_2_initial_price_push`, `module_3_periodic_actions`, `manual_price_push` (when overriding cart) |
| **Requires** | `setup_environment_2` (API credentials, shared with `push_prices_handler`), `requests`, `pandas`, `xlsxwriter` |
| **External** | MaxAB API (customized-cart endpoint), local filesystem (temp xlsx files) |
