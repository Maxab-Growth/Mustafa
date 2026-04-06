# Manual Price Push

## Purpose

On-demand tool for pushing manual price overrides for specific SKUs. Uses market data from the Market Data Module or fixed prices, computes packing unit prices, and pushes via the standard push handler. All 9 custom cohorts are covered automatically per product, with cohort-specific market prices.

---

## Flow

```mermaid
flowchart TD
    START[Define PUSH_LIST] --> LOAD[Load market data (v2) + WAC\n+ current prices + packing units\n+ target margins + stocks]
    LOAD --> LOOKUP["Build lookup table\nproduct × cohort with:\nmarket prices, WAC, target margin,\ncurrent price"]
    LOOKUP --> COMPUTE["For each product × cohort:\nresolve action → base price\n→ round to 0.25 EGP"]
    COMPUTE --> REVIEW["Review table:\nproduct, cohort, action,\ncurrent vs new price, margin"]
    REVIEW --> PUSH["push_prices via handler\n(auto-mirrors to main cohorts)"]
```

---

## Available Actions

| Action | Description | Price Source |
|--------|-------------|-------------|
| `market_min` | Lowest market price (minimum) | Cohort-specific market data |
| `market_25` | 25th percentile market price | Cohort-specific market data |
| `market_50` | Median market price (P50) | Cohort-specific market data |
| `market_75` | 75th percentile market price | Cohort-specific market data |
| `market_max` | Highest market price (maximum) | Cohort-specific market data |
| `market_avg` | Average of min and max market prices | Cohort-specific market data |
| `target_margin` | Price from brand-category target margin | WAC / (1 - margin) |
| `<number>` | Fixed price in EGP (e.g. `115`) | User-provided |
| `step_up` | Move price up one tier on the effective tier ladder (per cohort) | Tier ladder from market data + margins |
| `step_down` | Move price down one tier on the effective tier ladder (per cohort) | Tier ladder from market data + margins |

---

## Input Format

```python
PUSH_LIST = [
    (product_id, action),
    (6935, 'market_50'),       # median market price across all cohorts
    (5678, 115),               # fixed 115 EGP across all cohorts
    (4444, 'target_margin'),   # brand-category target margin
    (5555, 'step_up'),         # one tier up on ladder (per cohort)
    (5556, 'step_down'),       # one tier down on ladder (per cohort)
]
```

Each product is automatically expanded to all 9 cohorts (700-1126). Market-based actions use cohort-specific prices — Cairo may get a different price than Giza based on local competitor data. Fixed prices and target_margin are the same across all cohorts.

---

## Data Sources

| Data | Source | Key |
|------|--------|-----|
| Market prices | `get_market_data_v2()` via market_data_module | product_id, cohort_id |
| WAC | `finance.all_cogs` (current date window) | product_id |
| Current prices | `cohort_product_packing_units` + DBDP live slot | product_id, cohort_id |
| Packing units | `packing_unit_products` (60d sales-weighted) | product_id |
| Target margins | `performance.commercial_targets` (current/prev month) | brand, category |
| Stocks | `get_current_stocks()` with parent → child warehouse fallback (same mapping as queries module) | product_id, warehouse — **only SKUs with stock > 0** are included in the push set |

---

## Push Behavior

- Prices are pushed via `push_prices()` from `push_prices_handler.ipynb`
- Only warehouse–SKU rows with **stock > 0** are considered; SKUs with no sellable stock are omitted from the push
- Only SKUs where `new_price != current_price` are pushed
- Main/general cohorts (695, 61, 699, 697, 698, 696) are auto-mirrored
- Packing unit prices = base price x basic_unit_count
- Visibility rules applied (min PU hidden for multi-PU products)
- `MODE = 'testing'` prepares files without uploading; `MODE = 'live'` pushes to API

---

## Configuration

| Parameter | Value | Description |
|-----------|-------|-------------|
| `MODE` | `testing` / `live` | Controls whether prices are actually pushed |
| Rounding | 0.25 EGP | All prices rounded to nearest quarter |
| Default margin | 10% | Used when no brand-category target exists |

---

## Dependencies

| Direction | Module |
|-----------|--------|
| **Requires** | `market_data_module` (`get_market_data_v2`), `queries_module` (`get_current_stocks`), `push_prices_handler` (API push), `db.py` (queries), `constants.py` (cohorts, warehouses) |
| **External** | MaxAB API (price push), Snowflake (WAC, prices, products) |
