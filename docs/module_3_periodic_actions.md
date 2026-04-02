# Module 3 — Periodic Actions

## Purpose

Intraday UTH-based pricing engine running **5× daily** (12 PM, 3 PM, 6 PM, 9 PM, 12 AM Cairo). Compares real-time UTH (Up-To-Hour) performance against dynamic benchmarks and decides price changes, cart adjustments, SKU discounts, and quantity discounts. The primary responsive lever throughout the trading day.

---

## Full Decision Tree

```mermaid
flowchart TD
    START[Start: generate_periodic_action\nLoad extraction + UTH data] --> SKIP{OOS or\nbelow_min_stock?}
    SKIP -- Yes --> SKIP_OUT[Skip — no action]

    SKIP -- No --> CAPS[Check action caps]
    CAPS --> CAPS_CHK{"can_reduce: ≤ 3/day\ncan_increase: shared w/ M4"}

    CAPS_CHK --> ZD{zero_demand flag\nAND closing_stock_yesterday > 0?}

    %% ── ZERO DEMAND ──
    ZD -- Yes --> ZD_ACT[Zero Demand Path]
    ZD_ACT --> ZD_DISC["Activate SKU discount + QD\nWide cart: layer_3 or 150"]
    ZD_DISC --> ZD_PRICE{"Discounts already\nexist for SKU?"}
    ZD_PRICE -- Yes --> ZD_REDUCE["Price reduction via\ncalculate_induced_price"]
    ZD_PRICE -- No --> ZD_WAIT["Wait — discount first\nbefore price cut"]
    ZD_REDUCE --> POST
    ZD_WAIT --> POST

    %% ── HIGH DOH ──
    ZD -- No --> HDOH{"responsive_doh > 30\nAND inventory_value > 200\nAND not OOS yesterday?"}
    HDOH -- Yes --> HDOH_ACT[High DOH Path]
    HDOH_ACT --> HDOH_DISC["Activate SKU discount + QD"]
    HDOH_DISC --> HDOH_STAGE{"Staged approach:\ndiscounts added?"}
    HDOH_STAGE -- "No discounts yet" --> HDOH_WAIT[Add discounts first]
    HDOH_STAGE -- "Discounts exist" --> HDOH_PRICE{"qty_ratio vs 0.9?"}
    HDOH_PRICE -- "< 0.9" --> HDOH_REDUCE["Induced price reduction\ncalculate_induced_price"]
    HDOH_PRICE -- "≥ 0.9" --> HDOH_HOLD[Hold price]
    HDOH_WAIT --> POST
    HDOH_REDUCE --> POST
    HDOH_HOLD --> POST

    %% ── LOW STOCK ──
    HDOH -- No --> LSTOCK{Low stock\nprotected?}
    LSTOCK -- Yes --> LS_ACT["Hold price + cap cart\nAllow growth price increase"]
    LS_ACT --> POST

    %% ── ON TRACK ──
    LSTOCK -- No --> ONTRACK{"Both qty_ratio\nAND retailer_ratio\nin 0.9 – 1.1?"}
    ONTRACK -- Yes --> OT_ACT["Hold price\nRe-activate existing discounts"]
    OT_ACT --> POST

    %% ── RETAILERS GROWING, QTY ON TRACK ──
    ONTRACK -- No --> RET_GROW{"Retailers growing\nqty on track?"}
    RET_GROW -- Yes --> RG_CHK{"retailer_ratio > 1.2?"}
    RG_CHK -- Yes --> RG_INC[Price increase]
    RG_CHK -- No --> RG_HOLD[Hold]
    RG_INC --> POST
    RG_HOLD --> POST

    %% ── GROWING ──
    RET_GROW -- No --> GROWING{"qty_ratio > 1.1?\nGrowing"}
    GROWING -- Yes --> GR_ACT[Growing Path]
    GR_ACT --> GR_DISC["Remove top contributing discount"]
    GR_DISC --> GR_PRICE{"qty_ratio > 1.2?"}
    GR_PRICE -- Yes --> GR_UP["Price step up"]
    GR_PRICE -- No --> GR_HOLD2[Hold price]
    GR_UP --> GR_CART{"Qty spikes vs\nretailers?"}
    GR_HOLD2 --> GR_CART
    GR_CART -- "Qty ≫ Retailers" --> GR_TIGHTEN["Cart tightening\nby percentile"]
    GR_CART -- No --> POST
    GR_TIGHTEN --> POST

    %% ── DROPPING ──
    GROWING -- No --> DROP[Dropping Path]
    DROP --> DROP_CHK{"Which dimension\nis dropping?"}
    DROP_CHK -- "4A: Retailers weak" --> DROP_A["SKU discount activation\nCart increase ~25%"]
    DROP_CHK -- "4B: Qty weak" --> DROP_B["QD activation\nCart increase ~25%"]
    DROP_CHK -- "4C: Both weak" --> DROP_C["Price decrease\n+ SKU discount + QD\nCart increase ~25%"]
    DROP_A --> POST
    DROP_B --> POST
    DROP_C --> POST

    %% ── POST PROCESSING ──
    POST[Post-Processing]
    POST --> PP1["Floor enforcement"]
    PP1 --> PP2["Google Sheet fixed price/cart"]
    PP2 --> PP3["Push cart rules"]
    PP3 --> PP4["Push prices per cohort"]
    PP4 --> PP5["process_sku_discounts"]
    PP5 --> PP6["process_qd"]
    PP6 --> PP7["Archive → Snowflake + Slack"]
```

---

## UTH Target Calculation

```mermaid
flowchart LR
    A["p80_daily_240d × qtr_cntrb × uth_cntrb"] --> B["qty_target = max(result, turnover_target, 4)"]
    C["p70_retailers × min(in_stock_cntrb_ret, avg_uth_pct)"] --> D["retailer_target = max(result, 2)"]

    B --> E["qty_ratio = uth_qty / qty_target"]
    D --> F["retailer_ratio = uth_retailers / retailer_target"]

    E --> G{"> 1.1 → Growing\n0.9–1.1 → On Track\n< 0.9 → Dropping"}
    F --> G
```

| Component | Formula |
|-----------|---------|
| `uth_cntrb` | `min(in_stock_contribution_qty, avg_uth_pct)` |
| `qty_target` | `max(p80_daily_240d × qtr_cntrb × uth_cntrb, turnover_target, 4)` |
| `retailer_target` | `max(p70_retailers × min(in_stock_cntrb_ret, avg_uth_pct), 2)` |
| `qty_ratio` | `uth_qty / qty_target` |
| `retailer_ratio` | `uth_retailers / retailer_target` |
| Growing | ratio > 1.1 |
| On Track | 0.9 ≤ ratio ≤ 1.1 |
| Dropping | ratio < 0.9 |

---

## Key Functions

| Function | Description |
|----------|-------------|
| `generate_periodic_action` | Core engine — loads data, computes UTH targets, applies decision tree, triggers all downstream actions |
| `load_previous_actions` | Retrieves today's earlier M3 actions to enforce caps and detect oscillation |
| `load_module4_increases_today` | Checks M4 increases to enforce shared daily cap |
| `calculate_induced_price` | Computes a reduced price induced by discount existence (for zero demand / high DOH) |
| `adjust_cart_rule` | Adjusts cart by ±25% |
| `get_current_percentile_level` | Identifies which order-line percentile the current cart sits at |
| `get_next_lower_percentile` | Returns the next more restrictive percentile level |
| `is_cart_too_open` | Validates cart isn't excessively wide relative to order patterns |
| `find_next_price_above` | Next higher price on tier ladder |
| `find_next_price_below` | Next lower price on tier ladder |

---

## Inputs / Outputs

### Inputs
| Source | Data |
|--------|------|
| Snowflake — `Pricing_data_extraction` | Base SKU dataset with market data, inventory, margins |
| Snowflake — UTH queries | Today's cumulative performance (excl. current hour) |
| Snowflake — Previous actions | Today's M3 + M4 actions for cap enforcement |
| Google Sheets | Fixed price / cart overrides |

### Outputs
| Output | Destination |
|--------|-------------|
| Price changes | MaxAB API (per cohort) |
| Cart rule changes | MaxAB API |
| SKU discount instructions | → `sku_discount_handler` |
| QD instructions | → `qd_handler` |
| Action archive | Snowflake + Slack |

---

## Configuration

| Parameter | Value | Description |
|-----------|-------|-------------|
| `UTH_GROWING_THRESHOLD` | 1.10 | Ratio above which status = Growing |
| `UTH_DROPPING_THRESHOLD` | 0.90 | Ratio below which status = Dropping |
| `QTY_PRICE_INCREASE_THRESHOLD` | 1.2 | qty_ratio above which price increase allowed |
| `QTY_PRICE_DECREASE_THRESHOLD` | 0.8 | qty_ratio below which price decrease triggered |
| `MAX_PRICE_REDUCTIONS_PER_DAY` | 3 | Daily cap on price decreases per SKU |
| `CART_INCREASE_PCT` | 0.25 | Cart adjustment step (25%) |
| `CART_DECREASE_PCT` | 0.25 | Cart adjustment step (25%) |
| `LOW_STOCK_DOH_THRESHOLD` | 1 | DOH threshold for low-stock protection |
| `MIN_CART_RULE` | 10 | Minimum cart rule value |
| `MAX_CART_RULE` | 300 | Maximum cart rule value |

---

## Schedule

| Run | Time (Cairo) |
|-----|-------------|
| 1 | 12:00 PM |
| 2 | 3:00 PM |
| 3 | 6:00 PM |
| 4 | 9:00 PM |
| 5 | 12:00 AM |

---

## Dependencies

| Direction | Module |
|-----------|--------|
| **Requires** | `data_extraction` (Pricing_data_extraction), `queries_module` (UTH, stocks, percentiles), `setup_environment_2`, `common_functions` |
| **Triggers** | `sku_discount_handler`, `qd_handler` |
| **Coordinates with** | `module_4_hourly_updates` (shared increase cap) |
| **Archives to** | Snowflake, Slack |
