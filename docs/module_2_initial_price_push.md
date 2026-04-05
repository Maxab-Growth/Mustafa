# Module 2 — Initial Price Push

## Purpose

Daily baseline price reset running at ~6–8 AM Cairo time. Reads `Pricing_data_extraction` and computes target prices using market/margin tier ladders. Establishes the starting price and cart rule for every SKU before intraday modules take over.

---

## Decision Tree — Full Flow

```mermaid
flowchart TD
    START[Start: generate_initial_price_push\nRead Pricing_data_extraction] --> CHK_STOCK{stocks == 0?\nOut of Stock}

    %% ── OOS PATH ──
    CHK_STOCK -- Yes --> OOS[OOS Path]
    OOS --> OOS_PRICE["Price = get_max_price\nmax of market or margin ladder"]
    OOS_PRICE --> OOS_CART["Cart = P95 percentile"]
    OOS_CART --> FINAL

    %% ── ZERO DEMAND PATH ──
    CHK_STOCK -- No --> CHK_ZD{zero_demand?}
    CHK_ZD -- Yes --> ZD[Zero Demand Path]
    ZD --> ZD_CHK{Yesterday\nperformance?}
    ZD_CHK -- "Below On Track" --> ZD_DOWN2["2 steps down\nFloor: commercial_min_price"]
    ZD_CHK -- "Above On Track" --> ZD_HOLD[Hold price]
    ZD_CHK -- Else --> ZD_DOWN1["1 step down\nFloor: commercial_min_price"]
    ZD_DOWN2 --> ZD_CART["Cart = P95 percentile"]
    ZD_HOLD --> ZD_CART
    ZD_DOWN1 --> ZD_CART
    ZD_CART --> FINAL

    %% ── LOW STOCK PATH ──
    CHK_ZD -- No --> CHK_LS{doh ≤ 1\nAND stock > 0\nAND not zero_demand?}
    CHK_LS -- Yes --> LS[Low Stock Path]
    LS --> LS_CHK{combined status\nAbove or On Track?}
    LS_CHK -- Yes --> LS_UP["Allow 1 step up"]
    LS_CHK -- No --> LS_HOLD[Hold price]
    LS_UP --> LS_CART["Cart = P50 percentile\nlow DOH path"]
    LS_HOLD --> LS_CART
    LS_CART --> FINAL

    %% ── NORMAL PATH ──
    CHK_LS -- No --> NORMAL[Normal Path]
    NORMAL --> CHK_NODATA{No market/margin\ndata but has stock?}
    CHK_NODATA -- Yes --> TREAT_CRIT["Treat as Critical\ndecrease price"]
    CHK_NODATA -- No --> PRICE_ACTION["get_price_action\ncombined_status × yesterday_status"]

    TREAT_CRIT --> APPLY
    PRICE_ACTION --> APPLY[apply_price_action\nTier ladders + margin% fallback]
    APPLY --> MKT_SIG{Market signal override?\ndata_points ≥ 10\nvolatility ≤ 5%\nuptrend}
    MKT_SIG -- Yes --> MKT_BOOST["yesterday above → increase\nincrease → 2 steps + above-market fallback\nmarket boost tag"]
    MKT_SIG -- No --> CART_NORMAL["get_initial_cart_rule\npercentile-based"]
    MKT_BOOST --> CART_NORMAL
    CART_NORMAL --> FINAL

    %% ── FINAL ──
    FINAL[Apply fixed overrides\nGoogle Sheets price + cart]
    FINAL --> PUSH_CART[Push cart rules first]
    PUSH_CART --> PUSH_PRICE[Push prices per cohort\nMaxAB API]
    PUSH_PRICE --> ARCHIVE[Archive to Snowflake\npricing_initial_push]
```

---

## Price Action Matrix (Normal Path)

```mermaid
flowchart TD
    PA[get_price_action] --> C1{Combined: On Track\nYesterday: On Track}
    C1 -- Yes --> HOLD1[HOLD]

    PA --> C2{Combined: On Track\nYesterday: Above}
    C2 -- Yes --> INC1[INCREASE]

    PA --> C3{Combined: Above\nYesterday: Above\nnot on track}
    C3 -- Yes --> INC2[INCREASE]

    PA --> C4{Combined: Above\nYesterday: On Track}
    C4 -- Yes --> HOLD2[HOLD]

    PA --> C5{Combined: Below\nYesterday: Below}
    C5 -- Yes --> DEC1[DECREASE]

    PA --> C6{Combined: Below\nYesterday: Above}
    C6 -- Yes --> HOLD3["HOLD\noscillation guard"]

    PA --> C7{All other\ncombinations}
    C7 -- Yes --> HOLD4[HOLD]
```

---

## Tier System

```mermaid
flowchart LR
    A[Market margin tiers] --> B[get_enriched_market_tiers]
    C[Internal margin tiers] --> D[get_enriched_margin_tiers]
    B --> E[subdivide_tiers\nMax gap: 30% of target_margin]
    D --> E
    E --> F[Discrete price ladder\nMin step: 0.25 EGP]
    F --> G[find_next_price_above\nfind_next_price_below]
    G --> H[No ceiling - above-market fallback]
```

- Prices move on discrete ladders: market tiers first, then internal margin tiers
- Tiers subdivided when gap exceeds 30% of `target_margin`
- Minimum step size: **0.25 EGP**
- No ceiling cap on increases — when all tiers exhausted, above-market fallback kicks in (avg margin step → 20% target margin → +1% bump)

---

## Key Functions

| Function | Description |
|----------|-------------|
| `generate_initial_price_push` | Main engine — reads extraction data, applies decision tree, outputs price + cart actions |
| `get_price_action` | Maps `(combined_status, yesterday_status)` → hold / increase / decrease |
| `apply_price_action` | Executes the action using tier ladders with margin% fallback on increases |
| `find_next_price_above` | Finds the next higher price on the tier ladder |
| `find_next_price_below` | Finds the next lower price on the tier ladder |
| `get_initial_cart_rule` | Computes cart rule from order-line percentiles |
| `get_max_price` | Returns max of market ladder or margin ladder price (for OOS) |
| `get_market_tiers` | Extracts market-based tier ladder for a SKU |
| `get_margin_tiers` | Extracts internal margin-based tier ladder |
| `get_enriched_market_tiers` | Market tiers with interpolated steps |
| `get_enriched_margin_tiers` | Margin tiers with interpolated steps |
| `subdivide_tiers` | Splits tier gaps exceeding 30% of target margin |
| `get_margin_increase_pct` | Determines margin % step for increase actions |
| `get_above_market_price` | Fallback price when tier ladders exhausted (avg margin step / 20% target / +1%) |

---

## Inputs / Outputs

### Inputs
| Source | Data |
|--------|------|
| Snowflake — `Pricing_data_extraction` | Full SKU dataset (market data, inventory, performance, margins) |
| Google Sheets — "Fixed Price" | Product-level fixed price and fixed cart overrides |
| Market/Margin tier ladders | From `market_data_module` output embedded in extraction |

### Outputs
| Output | Destination |
|--------|-------------|
| Cart rule updates | MaxAB API (pushed first) |
| Price updates per cohort | MaxAB API |
| `pricing_initial_push` | Snowflake archive table |

---

## Configuration

| Parameter | Value | Description |
|-----------|-------|-------------|
| `PUSH_MODE` | `testing` / `live` | Controls whether prices are actually pushed |
| `LOW_STOCK_DOH_THRESHOLD` | 1 | DOH threshold for low-stock path |
| `MIN_CART_RULE` | 10 | Minimum allowed cart rule |
| `MAX_CART_RULE` | 500 | Maximum allowed cart rule |
| `MIN_PRICE_CHANGE_EGP` | 0.25 | Smallest allowed price change |
| Tier subdivision threshold | 30% of `target_margin` | Max gap before tiers are subdivided |
| Market signal: min data points | 10 | Required for market signal override (yesterday above on track triggers hold→increase) |
| Market signal: max volatility | 5% | Volatility ceiling for signal eligibility |

---

## Dependencies

| Direction | Module |
|-----------|--------|
| **Requires** | `data_extraction` (Pricing_data_extraction table), `market_data_module` (tier ladders), `common_functions` (API upload, Slack), `setup_environment_2` |
| **External** | MaxAB API (price + cart push), Google Sheets (fixed overrides) |
| **Archives to** | Snowflake — `pricing_initial_push` |
