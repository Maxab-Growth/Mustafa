# Market Data Module

## Purpose

Shared data layer that collects competitor and market prices from three independent sources (Ben Soliman, Marketplace, Scraped) and builds a unified margin context for downstream pricing decisions. Requires no caller inputs — all data is sourced directly from Snowflake. Produces market price bands, margin tiers, brand-level fallbacks, and technical market signals consumed by every other module.

---

## Flow Diagram

```mermaid
flowchart TD
    A[Start: get_market_data] --> B1[Query Ben Soliman\nReference prices]
    A --> B2[Query Marketplace\nRegional shelf prices]
    A --> B3[Query Scraped\nCompetitor prices]

    B1 --> C[Join all sources\non SKU + warehouse]
    B2 --> C
    B3 --> C

    C --> D[Build commercial groups\nWeighted median prices]
    D --> E[Compute WAC / margins / targets]
    E --> F{Coverage filter\ntotal_p ≥ 2?}
    F -- No --> G[Exclude SKU\ninsufficient data]
    F -- Yes --> H[price_analysis\nmin / P25 / P50 / P75 / max]
    H --> I[calculate_step_bounds]
    I --> J[Convert to margin columns\nbelow_market → above_market]
    J --> K[Return market data]

    subgraph Parallel Enrichments
        L[get_margin_tiers\nHistorical realized margins\nIQR-cleaned, time-weighted]
        M[get_brand_market_percentiles\nRegion × brand × category\nmargin percentiles]
        N[get_market_signals\n60-day technical indicators\nSMAs, trend, momentum, volatility]
    end

    K --> O[fill_brand_market_fallback\nMap brand percentiles when\nSKU-level data missing]
    M --> O
    O --> P[Set market_data_source\nsku / brand / null]
```

---

## Price Sources

| Source | Description | Filtering | Fallback |
|--------|-------------|-----------|----------|
| **Ben Soliman** | Reference prices, COGS sanity check vs WAC, main vs lower price tracks | — | — |
| **Marketplace** | Regional online shelf prices | ±40% WAC filter, IQR cleaning | Regional fallback |
| **Scraped** | Competitor prices matched to SKUs | Regional fallback priorities | Percentile-based |

**Coverage rule:** `total_p ≥ 2` — Points: Ben = 1, Marketplace = 1–3, Scraped = 1–5.

---

## Key Functions

| Function | Description |
|----------|-------------|
| `get_market_data()` | End-to-end pipeline: query 3 sources → join → commercial groups (weighted median) → WAC/margins/targets → coverage filter → price analysis → step bounds → margin columns |
| `get_market_data_v2()` | V2 pipeline producing sorted price tier lists. Single-price SKUs expanded via: (1) regional fallback — borrow prices from neighboring regions, (2) margin-step expansion — ±2 steps centered on the single price. Also includes commercial price-up induced prices and brand fallback |
| `get_margin_tiers()` | Historical realized margins by warehouse × product. IQR-cleaned, time-weighted with exponential decay. Produces 8 tiers from `min_boundary` to `max_boundary` |
| `get_brand_market_percentiles()` | Region × brand × category margin percentiles used as fallback when SKU-level data is missing |
| `fill_brand_market_fallback()` | Maps brand percentiles to margin/price columns; sets `market_data_source` to `'sku'`, `'brand'`, or `null` |
| `get_market_signals()` | 60-day technical indicators from `Pricing_data_extraction`: SMAs, trend direction, momentum, volatility |

---

## Margin Tiers Detail

```mermaid
flowchart LR
    A[Daily margins\n per warehouse × product] --> B[IQR cleaning\nRemove outliers]
    B --> C[Same-quarter filter]
    C --> D[Time-weighted\nexp −0.023 × days_ago]
    D --> E[8 tiers\nmin_boundary → max_boundary]
    D --> F[Optimal margin\n120d maximizing smoothed\nweighted gross profit]
```

- Exponential decay factor: `exp(-0.023 × days_ago)`
- Optimal margin: 120-day window maximizing smoothed weighted gross profit

---

## Inputs / Outputs

### Inputs
| Source | Data |
|--------|------|
| Snowflake — Ben Soliman | Reference prices, COGS |
| Snowflake — Marketplace | Regional shelf prices |
| Snowflake — Scraped | Competitor matched prices |
| Snowflake — Pricing_data_extraction | 60-day price history for signals |
| Snowflake — Daily margins | Realized margin history |

### Outputs
| Output | Description |
|--------|-------------|
| Market data DataFrame | Price bands (min/P25/P50/P75/max), margin columns (below_market → above_market), step bounds |
| Margin tiers DataFrame | 8-tier ladder per warehouse × product |
| Brand percentiles DataFrame | Region × brand × category margin percentiles |
| Market signals DataFrame | SMA, trend, momentum, volatility per SKU |
| `market_data_source` flag | `'sku'` / `'brand'` / `null` per row |

---

## Configuration

| Parameter | Value | Description |
|-----------|-------|-------------|
| Coverage threshold | `total_p ≥ 2` | Minimum source score to include a SKU |
| Ben Soliman weight | 1 point | Coverage contribution |
| Marketplace weight | 1–3 points | Coverage contribution |
| Scraped weight | 1–5 points | Coverage contribution |
| Decay constant | 0.023 | Exponential decay for time-weighting margins |
| Optimal margin window | 120 days | Lookback for gross-profit-maximizing margin |
| Marketplace WAC filter | ±40% | Reject shelf prices outside this band |

---

## Dependencies

| Direction | Module |
|-----------|--------|
| **Requires** | `setup_environment_2` (environment configuration) |
| **Consumed by** | `data_extraction`, `module_2_initial_price_push`, `module_3_periodic_actions`, `module_4_hourly_updates` |
