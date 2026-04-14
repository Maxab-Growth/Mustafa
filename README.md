# MaxAB Pricing Automation System

Automated SKU-level pricing engine for **MaxAB Egypt** — managing prices, cart rules, SKU discounts, and quantity discounts across 12 warehouses and 8 cohorts. The system runs on a daily + intraday cycle, pulling data from Snowflake, PostgreSQL, Google Sheets, and competitor sources, then pushing changes to the MaxAB API.

---

## Module Overview

| Module | File | Schedule (Cairo) | Purpose |
|--------|------|-------------------|---------|
| Market Data V1 (legacy) | `market_data_module.ipynb` | Daily, pre-pipeline | Legacy competitor/market prices; builds margin tiers and brand fallbacks for DB storage |
| Market Data V2 (price tiers) | `market_data_module_2.ipynb` | Daily, pre-pipeline | Dual output: runs V1 internally for legacy DB + adds V2 sorted price tiers per (product, region) |
| Data Extraction | `data_extraction.ipynb` | Daily, 8:00 AM | Builds wide warehouse-SKU dataset from 20+ Snowflake queries |
| Module 2 — Initial Price Push | `module_2_initial_price_push.ipynb` | Daily, 8:00 AM (after extraction) | Baseline price & cart rule reset using performance + market tiers |
| Module 3 — Periodic Actions | `module_3_periodic_actions.ipynb` | 12 PM, 5 PM, 11 PM | UTH-based intraday price/discount/cart adjustments |
| Module 4 — Hourly Updates | `module_4_hourly_updates.ipynb` | 1–3 AM, 9–11 AM, 1–3 PM, 4–10 PM | WAC-driven and growth-based hourly price tweaks |
| Module 5 — New Intros & Invisible | `module_5_new_intros_invisible.ipynb` | 4:00 PM | First-time pricing for new and invisible SKUs |
| QD Handler | `qd_handler.ipynb` | Called by Module 3 | Quantity discount lifecycle (deactivate → create 3-tier QDs) |
| SKU Discount Handler | `sku_discount_handler.ipynb` | Called by Module 3 | Per-SKU special discount lifecycle via S3 bulk upload |
| Queries Module | `queries_module.ipynb` | Shared library | Centralized data access layer (Snowflake, PostgreSQL, Sheets) |
| Manual Price Push | `manual_price_push.ipynb` | On-demand | Manual price overrides with `step_up`/`step_down` actions; stock > 0 filter |

---

## Architecture

```mermaid
graph TB
    subgraph Data Sources
        SF[(Snowflake)]
        PG[(PostgreSQL DWH)]
        GS[Google Sheets]
        COMP[Competitor Sources]
        CT[Commercial Tool]
    end

    subgraph Shared Layer
        QM[Queries Module]
        MD[Market Data V1]
        MD2[Market Data V2]
        CF[common_functions.py]
    end

    subgraph Daily Pipeline
        DE[Data Extraction]
        M2[Module 2 — Initial Price Push]
    end

    subgraph Intraday Pipeline
        M3[Module 3 — Periodic Actions]
        M4[Module 4 — Hourly Updates]
        QD[QD Handler]
        SKU[SKU Discount Handler]
    end

    subgraph Standalone
        M5[Module 5 — New Intros & Invisible]
        MP[Manual Price Push]
    end

    subgraph Targets
        API[MaxAB API]
        S3[AWS S3]
        SL[Slack]
    end

    SF & PG & GS & COMP --> QM
    QM --> MD
    MD & CT --> MD2
    QM & MD2 --> DE
    DE -->|Pricing_data_extraction| M2
    QM & MD2 --> M3
    QM & MD2 --> M4
    M3 --> QD
    M3 --> SKU
    QM --> M5

    M2 -->|cart rules → prices| API
    M3 -->|cart → prices → discounts| API
    M4 -->|prices + cart rules| API
    M5 -->|per-cohort Excel| API
    MP -->|per-cohort Excel| API
    QD -->|Excel upload| API
    SKU -->|S3 bulk upload| S3
    S3 --> API
    DE -->|notification| SL
```

---

## Daily Pipeline Flow

```mermaid
flowchart LR
    A["01:00–03:00<br/>Module 4<br/>Hourly"] --> B["08:00<br/>Data Extraction +<br/>Module 2"]
    B --> C["09:00–11:00<br/>Module 4<br/>Hourly + Treasure Hunt"]
    C --> D["12:00<br/>Module 3<br/>Periodic #1"]
    D --> E["13:00–14:00<br/>Module 4<br/>Hourly"]
    E --> F["15:00<br/>Module 4<br/>+ Savvy Update"]
    F --> G["16:00<br/>Module 5 +<br/>Module 4"]
    G --> H["17:00<br/>Module 3<br/>Periodic #2"]
    H --> I["18:00–22:00<br/>Module 4<br/>Hourly"]
    I --> J["23:00<br/>Module 3<br/>Periodic #3"]

    style A fill:#d97706,color:#fff
    style B fill:#2563eb,color:#fff
    style C fill:#d97706,color:#fff
    style D fill:#059669,color:#fff
    style E fill:#d97706,color:#fff
    style F fill:#d97706,color:#fff
    style G fill:#8b5cf6,color:#fff
    style H fill:#059669,color:#fff
    style I fill:#d97706,color:#fff
    style J fill:#059669,color:#fff
```

---

## Module Details

### 1. Market Data Module (V1 + V2)

**Files:** `modules/market_data_module.ipynb` (V1 legacy) · `modules/market_data_module_2.ipynb` (V2 price tiers)

The production entry point is **`market_data_module_2.ipynb`**, which runs V1 internally to preserve legacy DB output and layers V2 functions on top.

**Dual output architecture:**

| Function | Output | Consumer |
|----------|--------|----------|
| `get_market_data_legacy()` | Percentiles (P25/P50/P75/max) per SKU — same format as V1 | DB storage, backward-compatible consumers |
| `get_market_data_v2()` | Sorted `price_tiers` list per `(product_id, region)` | All pricing modules via `effective_tiers` |

**Data sources (V2):**

- **Ben Soliman** — cleaned query with WAC sanity check on the lower track
- **Marketplace** — competitor marketplace prices
- **Scraped** — Speed = Alex only

**V2 enrichment pipeline:**

1. **Brand fallback** — Python-side fallback for SKUs without any market data, using brand-level aggregates
2. **Single-price expansion** — when only one competitor price exists, synthetic tiers are generated
3. **Margin anchor injection** — for SKUs with actual market data (`market_data_source = 'sku'`), two anchor prices are injected: **target margin price** (`wac / (1 - target_margin)`) and **ATH margin price** (`wac / (1 - ath_margin)`, 240-day IQR-filtered max across warehouses). These anchor the "comfort zone" so the subsequent subdivision creates fine-grained steps between the planned margin, the proven historical ceiling, and high market prices
4. **Step subdivision** — tiers are subdivided when the gap implies > 30% target margin
5. **Commercial price-up induced prices** — sourced from `retool.stocking_request`, injected as additional tier anchors
6. **Final rounding + dedup** — all prices rounded to nearest 0.25 EGP, duplicates removed

All downstream modules consume `effective_tiers` = `price_tiers` (from V2) > `margin_tier_prices` (from historical margins) > empty list.

### 2. Data Extraction

**File:** `data_extraction.ipynb`

Builds a comprehensive warehouse × SKU dataset by joining 20+ Snowflake queries: product base, WAC costs, market data (via `get_market_data_legacy()` from the V2 module — same DB output as V1), 120-day NMV sales, margin stats/targets, inventory, demand signals, PO/leadtime, active discounts, cart rules, and ABC classification. Computes derived fields like `price_position`, `performance_tag`, `DOH`, `running_rate`, and `ABC_class`. Exports to Excel, writes to Snowflake table `Pricing_data_extraction`, and sends a Slack notification on completion.

### 3. Module 2 — Initial Price Push

**File:** `modules/module_2_initial_price_push.ipynb` | **Schedule:** Daily 8:00 AM Cairo

Performs the baseline price and cart rule reset each morning. Reads from `Pricing_data_extraction` and walks a decision tree per warehouse-SKU:

```mermaid
flowchart TD
    START([Read Pricing_data_extraction]) --> OOS{Stock = 0?}
    OOS -->|Yes| OOSA[Price → max tier<br/>Cart → P95]
    OOS -->|No| ZERO{Zero demand?}
    ZERO -->|Yes| ZEROA[Step down 1–2 tiers<br/>Floor = commercial min]
    ZERO -->|No| LOW{DOH ≤ 1?}
    LOW -->|Yes| LOWA[Allow step up if performing well<br/>Else hold]
    LOW -->|No| NORMAL[Performance-based decision]
    NORMAL --> PERF{Combined + Yesterday Status}
    PERF -->|Strong| UP[Price increase + market boost]
    PERF -->|Stable| HOLD[Hold price]
    PERF -->|Weak| DOWN[Price decrease]
    UP & HOLD & DOWN --> CART[Set cart from percentiles<br/>P50 / P95 / layer_1]
    OOSA & ZEROA & LOWA --> CART
    CART --> OVERRIDE[Apply fixed overrides<br/>from Google Sheets]
    OVERRIDE --> PUSH[Push cart rules → then prices<br/>per cohort via API]
```

Target prices are computed from `effective_tiers` in discrete steps (minimum 0.25 EGP). No ATH ceiling cap — prices can step beyond the top of the tier ladder via `get_above_market_price()` (avg margin step / 20% target margin / +1% bump). Market signal: when yesterday is **above on track** and market is trending up, the action is upgraded to an increase **regardless of the combined status**.

**Commercial price-up fallback:** when no market signal is available, the commercial price-up percentage from `retool.stocking_request` drives a tiered signal:

| Price-up % | Signal |
|------------|--------|
| < 5% | None |
| 5–15% | UPTREND |
| ≥ 15% | STRONG UPTREND |

**Above-market fallback:** when `effective_tiers` are exhausted, `get_above_market_price()` computes the next price as avg margin step / 20% target margin / +1% bump.

Fixed price and cart overrides from Google Sheets are applied last. Push order is always **cart rules first, then prices**.

### 4. Module 3 — Periodic Actions

**File:** `modules/module_3_periodic_actions.ipynb` | **Schedule:** 12 PM, 5 PM, 11 PM

The main intraday engine. Compares up-till-hour (UTH) quantity and retailer counts against dynamic benchmarks (P80 for both quantity and retailers: daily benchmark × quarterly contribution × hour contribution, with the same P80 lookback window — retailer column is still named `p70` but uses P80 calculation). Uses fixed ratio thresholds (0.9 / 1.1) to classify each SKU as **Growing**, **On Track**, or **Dropping**.

```mermaid
flowchart TD
    UTH([Fetch UTH Performance]) --> CMP{Compare to Benchmarks}
    CMP -->|qty_ratio > 1.1<br/>ret_ratio > 1.1| GR[Growing]
    CMP -->|Within ±10%| OT[On Track]
    CMP -->|qty_ratio < 0.9<br/>or ret_ratio < 0.9| DR[Dropping]
    CMP -->|qty = 0| ZD[Zero Demand]

    GR --> GR1[Remove highest-contributing<br/>discount tier]
    GR --> GR2{qty_ratio > 1.2?}
    GR2 -->|Yes| GR3[Price step up]
    GR --> GR4[Cart tightening if<br/>qty per retailer spikes<br/>ratio > 1.3]

    DR --> DR1[Activate SKU discount + QD]
    DR --> DR2[Cart increase ~25%]
    DR --> DR3[Price decrease<br/>branches 4A / 4B / 4C]

    ZD --> ZD1[SKU + QD activation<br/>Wide cart<br/>Induced price reduction]

    GR1 & GR3 & GR4 & DR1 & DR2 & DR3 & ZD1 --> POST[Post-processing]
    OT --> POST

    POST --> FLOOR[Enforce price floors]
    FLOOR --> FIX[Apply fixed overrides]
    FIX --> PUSH[Push: cart → prices →<br/>SKU discounts → QDs]

    subgraph Special Cases
        HDOH{DOH > 30 &<br/>inv value > 200?}
        HDOH -->|Yes| STAGED[Staged discount + price approach]
        LOWS{Low stock?}
        LOWS -->|Yes| CAPS[Hold + cap cart]
    end
```

**Daily caps:** max 3 price reductions per SKU; shared increase cap with Module 4.

**Key behaviors:**

- Fresh commercial min prices fetched each run via `get_commercial_min_prices()`
- Price floor derived from `effective_tiers[0]` (not legacy `market_min`)
- `effective_tiers` passed to both SKU discount and QD handlers
- Cart tightening triggers when `qty_per_retailer_ratio > 1.3`

**Post-processing:** price floor enforcement, fixed overrides, then push sequence: cart rules → prices → SKU discounts → QDs.

### 5. Module 4 — Hourly Updates

**File:** `modules/module_4_hourly_updates.ipynb` | **Schedule:** 1–3 AM, 9–11 AM, 1–3 PM, 4–10 PM

Runs on all non-Module-3 hours for fine-grained adjustments. Uses fixed ratio thresholds (0.9/1.1), aligned with Module 3. Consumes `effective_tiers` in its action pipeline and fetches fresh commercial min prices each run via `get_commercial_min_prices()`.

| Trigger | Action |
|---------|--------|
| WAC increase > 0.5% (today's purchases) | Restore margin — price step up |
| UTH + last-hour both growing | Smooth price step up |
| Retailer-only growth | Conservative step up |
| Qty-only growth | Step up with caution |
| Commercial min violation | Enforce floor |
| Qty > 2× target | Cart rule tightening |

**Coordination rules:** 2-hour cooldown after Module 3, 1-hour self-cooldown, max 7 qty-driven price steps per day.

### 6. Module 5 — New Intros & Invisible

**File:** `modules/module_5_new_intros_invisible.ipynb` | **Schedule:** Daily 4:00 PM Cairo

Handles SKUs that need first-time pricing: **new intros** (stock present, no cohort price or invisible packing units). Price is calculated as:

```
price = WAC × basic_unit_count / (1 - margin)   →   rounded to nearest 0.25 EGP
```

Margin is resolved via hierarchy: brand + category target → category target → default 10%. Pushes per-cohort Excel files to the MaxAB API.

### 7. QD Handler

**File:** `modules/qd_handler.ipynb` | **Called by:** Module 3

Manages the full quantity discount lifecycle — deactivates all active QDs via API, then creates new ones. Prefers `effective_tiers` for tier price derivation; falls back to individual market/margin columns when tiers are unavailable.

| Tier | Source | Elasticity Ratio | Discount Cap |
|------|--------|-------------------|--------------|
| T1 | Market/margin ladder | 1.1 | 4% |
| T2 | Market/margin ladder | 3.0 | 5% |
| T3 | Wholesale (car-cost savings) | — | 6% |

Top 400 tier entries per warehouse by inventory value. QDs are uploaded via Excel to the MaxAB API. Cart rules are aligned to match the max tier quantity. Duration: **14 hours** from creation.

### 8. SKU Discount Handler

**File:** `modules/sku_discount_handler.ipynb` | **Called by:** Module 3

Per-SKU "Special Discounts" lifecycle: deactivate existing → create new ones via S3 bulk upload. Discount range: **0.25–5%** of effective price. Prefers `effective_tiers` for price-aware discounting; falls back to individual market/margin columns when tiers are unavailable.

| Condition | Behavior |
|-----------|----------|
| Zero demand | Aggressive discounting |
| Overstock (DOH > 30) | Moderate discount push |
| Low stock | Protected — minimal/no discount |
| Normal (UTH-based) | On track / dropping / growing logic |

**Retailer targeting:** churned/dropped, category-not-product buyers, view-no-orders, out-of-cycle. Wholesale and inactive retailers are excluded. Duration: **14 hours**. Upload limits: 100 retailers per chunk, 1000 rows per file.

### 9. Queries Module

**File:** `modules/queries_module.ipynb`

Shared data access layer used by every module. Centralizes all external queries:

- **Snowflake:** stocks, prices, WAC, cart rules, packing units, UTH performance, hourly distribution, stock snapshots, percentiles, quarterly contribution, target turnover, **`get_commercial_min_prices()`** (live commercial minimum constraints from `finance.minimum_prices` for Module 3 and Module 4 each run), and **`get_commercial_price_ups()`** (price-up percentages from `retool.stocking_request` used for commercial price-up signals)
- **PostgreSQL (DWH):** last-hour performance
- **Google Sheets:** fixed prices and cart rules (manual overrides)
- **Retailer selection:** churned buyers, category buyers, out-of-cycle, view-no-orders, exclusion lists

---

## Key Concepts

### Cohorts & Warehouses

MaxAB organizes Egypt into regional cohorts, each mapped to one or more physical warehouses. Pricing and cart rules are set **per cohort per SKU**.

| Region | Cohort ID | Warehouses |
|--------|-----------|------------|
| Cairo | 700 | Mostorod |
| Giza | 701 | Barageel, Sakkarah |
| Alexandria | 702 | Khorshed Alex |
| Delta West | 703 | El-Mahala, Tanta |
| Delta East | 704 | Mansoura FC, Sharqya |
| Upper Egypt — Menya | 1123 | Menya Samalot |
| Upper Egypt — Assiut | 1124 | Assiut FC |
| Upper Egypt — Sohag | 1125 | Sohag |
| Upper Egypt — Beni Suef | 1126 | Bani Sweif |

**Main/General Cohort Mirroring:** Prices pushed to custom cohorts are automatically mirrored to their main/general counterparts by the push handler:

| Main Cohort | Mirrors From |
|-------------|-------------|
| 695 | 700 (Cairo) |
| 61 | 700 (Cairo) |
| 699 | 702 (Alexandria) |
| 697 | 703 (Delta West) |
| 698 | 704 (Delta East) |
| 696 | 1123 (Upper Egypt - Menya) |

### WAC (Weighted Average Cost)

The cost basis for all margin calculations. WAC reflects the blended purchase cost across recent POs and is updated intraday when new purchases arrive. Module 4 specifically watches for WAC jumps > 0.5% to restore margin.

### Effective Tiers (V2 Pricing Ladder)

All pricing modules operate on a unified **`effective_tiers`** list rather than raw market or margin columns. The resolution order:

1. **`price_tiers`** — sorted price list from Market Data V2 (`get_market_data_v2()`), built from competitor sources + margin anchors (target margin + ATH margin) + commercial price-up anchors, all rounded to 0.25 EGP
2. **`margin_tier_prices`** — fallback ladder derived from historical margin boundaries (min → max margin, split by ABC class)
3. **Empty list** — when neither source provides data

```mermaid
graph LR
    PT["price_tiers<br/>(V2 market data)"] -->|available?| YES1[Use as effective_tiers]
    PT -->|empty| MT["margin_tier_prices<br/>(historical margins)"]
    MT -->|available?| YES2[Use as effective_tiers]
    MT -->|empty| EMPTY["empty list<br/>(no tiers)"]
```

Prices follow **discrete 0.25 EGP steps** within the effective tier list. Modules move up or down by 1–2 steps based on performance signals. There is **no ceiling cap** — when the top tier is exhausted, the above-market fallback (`get_above_market_price()`) computes the next price from avg margin step / 20% target margin / +1% bump.

### UTH (Up-Till-Hour) Performance

Cumulative sales metrics (quantity sold, unique retailers) from midnight to the current hour, compared against dynamic benchmarks. Benchmarks use **P80 for both quantity and retailers**, combined with quarterly seasonality and hourly distribution patterns. This drives all Module 3 and Module 4 decisions.

### Performance Statuses

Used by Module 2 for the daily baseline. Based on 120-day NMV achievement vs target:

| Status | Meaning |
|--------|---------|
| Star Performer | Consistently exceeds target |
| Over Achiever | Above target |
| On Track | Within ±10% of target |
| Underperforming | Below target |
| Struggling | Significantly below target |
| Critical | Deep underperformance |

### ABC Classification

SKUs are classified by order count contribution (descending):

| Class | Cumulative Share | Pricing Stance |
|-------|-----------------|----------------|
| A | ≤ 30% | Aggressive (lower market percentile, tighter margins) |
| B | ≤ 75% | Balanced |
| C | Remainder | Conservative (higher margins, wider cart rules) |

### DOH (Days on Hand)

```
DOH = current_stock / running_rate
```

Running rate is derived from recent sales velocity. DOH drives stock-aware decisions: low DOH (≤ 1) triggers protective holds; high DOH (> 30 with significant inventory value) triggers staged discounting.

### Price Position

Describes where the current SKU price sits relative to market data:

**Below Market → At Min → Below Median → At Median → Above Median → At Max → Above Market**

Used by modules to determine headroom for increases or urgency for decreases.

### Cart Rules

Minimum purchase quantity per SKU per cohort. Controls the smallest order a retailer can place. Cart rules are always pushed **before** prices to avoid transient states where a low cart and high price coexist. Typical sources: historical percentiles (P50, P95), normal refill + std deviation bands.

### Push Sequence

All modules follow the same push order to the MaxAB API:

1. **Cart rules** (set minimum purchase quantities)
2. **Prices** (set per-cohort SKU prices)
3. **SKU discounts** (Module 3 only)
4. **Quantity discounts** (Module 3 only)

This order ensures cart rules are in place before any price change takes effect.

---

## Tech Stack

| Component | Role |
|-----------|------|
| **Snowflake** | Primary data warehouse — market data, sales, inventory, benchmarks |
| **PostgreSQL** | DWH for last-hour performance data |
| **MaxAB API** | Target system — receives price, cart, and discount pushes |
| **Google Sheets** | Manual overrides for fixed prices and cart rules |
| **AWS S3** | Bulk upload channel for SKU discounts |
| **AWS Secrets Manager** | Credential storage for all external services |
| **Slack** | Pipeline notifications and alerts |
| **Python** | Runtime — Jupyter notebooks |
| Key libraries | `pandas`, `numpy`, `snowflake-connector-python`, `boto3`, `requests`, `openpyxl`, `gspread` |

---

## Data Flow

```mermaid
flowchart TD
    subgraph External["External Data Sources"]
        SF[(Snowflake)]
        PG[(PostgreSQL)]
        GS[Google Sheets]
        CT[Commercial Tool]
    end

    subgraph Shared["Shared Layer"]
        QM[queries_module.ipynb]
        MD[market_data_module.ipynb<br/>V1 legacy]
        MD2[market_data_module_2.ipynb<br/>V2 price tiers]
    end

    subgraph Daily["Daily Batch — 08:00"]
        DE[data_extraction.ipynb]
        PDE[(Pricing_data_extraction<br/>Snowflake table)]
        M2[Module 2<br/>Initial Price Push]
    end

    subgraph Intraday["Intraday — 09:00–23:00"]
        M3[Module 3<br/>Periodic Actions]
        M4[Module 4<br/>Hourly Updates]
        SKUD[SKU Discount Handler]
        QDH[QD Handler]
    end

    subgraph Standalone["Standalone"]
        M5[Module 5<br/>New Intros]
    end

    subgraph Output["MaxAB Platform"]
        API[MaxAB API]
        S3[AWS S3]
    end

    SF & PG & GS --> QM
    SF --> MD
    SF & CT --> MD2
    MD --> MD2
    QM & MD2 --> DE
    DE --> PDE
    PDE --> M2
    M2 -->|cart + prices| API

    QM & MD2 --> M3
    M3 -->|cart + prices| API
    M3 --> SKUD
    M3 --> QDH
    SKUD -->|S3 bulk| S3 --> API
    QDH -->|Excel upload| API

    QM --> M4
    M4 -->|cart + prices| API

    QM --> M5
    M5 -->|per-cohort Excel| API
```

---

## Configuration

Key parameters (defined inline in each module):

| Parameter | Value | Description |
|-----------|-------|-------------|
| `MODULE_2_RUN_TIME` | 08:00 | Daily baseline push (Cairo) |
| `MODULE_3_RUN_TIMES` | 12, 17, 23 | Periodic action windows |
| `MIN_CART_RULE` | 2 | Absolute minimum cart (units) |
| `MAX_CART_RULE` | 150 | Cart ceiling |
| `MIN_PRICE_REDUCTION_PCT` | 0.25% | Smallest allowed price cut |
| `ON_TRACK_THRESHOLD` | ±10% | Band for "On Track" status |
| `P80_BENCHMARK_DAYS` | 240 | Lookback for quantity and retailer benchmarks (P80, shared window) |
| `HOURLY_PATTERN_DAYS` | 120 | Lookback for hourly distributions |
| `TIMEZONE` | Africa/Cairo | All schedules in Cairo time |

ABC-class-specific settings:

| Setting | A | B | C |
|---------|---|---|---|
| Market percentile | P25 | P50 | P75 |
| Margin percentile | 50% | 75% | 90% |
| Cart std multiplier | 1× | 2× | 5× |

---

## Project Structure

```
Mustafa/
├── modules/
│   ├── market_data_module.ipynb        # V1 legacy market data
│   ├── market_data_module_2.ipynb      # V2 price tiers (production entry point)
│   ├── module_2_initial_price_push.ipynb
│   ├── module_3_periodic_actions.ipynb
│   ├── module_4_hourly_updates.ipynb
│   ├── module_5_new_intros_invisible.ipynb
│   ├── queries_module.ipynb            # Shared data access
│   ├── qd_handler.ipynb               # Quantity discounts
│   ├── sku_discount_handler.ipynb      # SKU special discounts
│   ├── push_prices_handler.ipynb       # API push helper
│   └── push_cart_rules_handler.ipynb
├── docs/                               # Per-module documentation
├── queries/                            # Standalone SQL reference queries
├── Mapping/                            # SKU mapping pipeline
│   └── bs_mapping_pipeline.ipynb       # Ben Soliman SKU mapping
├── archive/                            # Inactive/legacy notebooks
├── constants.py                        # Shared constants (warehouses, cohorts, channels)
├── db.py                               # Shared query_snowflake() implementation
├── common_functions.py                 # AWS secrets, Slack, Snowflake upload
├── setup_environment_2.py              # Environment + DB credentials
├── data_extraction.ipynb               # Daily data build
├── scheduler.ipynb                     # Orchestrator
├── cohort_700_sku_sheet_updater.ipynb  # Weekly Savvy sheet update
├── treasure_hunt_scheduler.ipynb       # Treasure hunt pricing
├── whole_sale_new_logic.ipynb          # Wholesale pricing
├── manual_price_push.ipynb          # Manual price overrides tool
├── requirements.txt                    # Python dependencies
└── README.md
```

---

## Credentials & Secrets

All credentials are managed through **AWS Secrets Manager** (region: `us-east-1`). The `common_functions.py` module provides `get_secret()` and `initialize_env()` to load them into environment variables at runtime. No credentials are stored in code or config files.

Required secrets: Snowflake credentials, PostgreSQL DWH credentials, MaxAB API keys, Google Sheets service account, Slack webhook URL, S3 access keys.
