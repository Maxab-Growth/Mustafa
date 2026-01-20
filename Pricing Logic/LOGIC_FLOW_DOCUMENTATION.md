# Pricing Action Engine - Logic Flow Documentation

## Overview

This document explains the complete logic flow for the automated pricing action engine. The system analyzes SKU performance, stock levels, and discount contributions to generate actionable recommendations.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      DATA EXTRACTION (data_extraction.ipynb)                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌─────────────────┐  │
│  │ Market Data  │  │ Internal     │  │ Stock &      │  │ Performance     │  │
│  │ (Margins)    │  │ Margins      │  │ Running Rate │  │ Status          │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  └─────────────────┘  │
│                              ↓                                               │
│                    pricing_with_discount.xlsx                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                   ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ACTION ENGINE (pricing_action_engine.ipynb)               │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                         CONDITION EVALUATION                           │ │
│  │                                                                        │ │
│  │  1. Zero Demand + Stock > 0                                            │ │
│  │  2. Star Performer / Over Achiever                                     │ │
│  │  3. On Track + Stock > 0                                               │ │
│  │  4. Struggling / Underperforming / Critical + Stock > 0                │ │
│  │  5. No Data + Stock > 0                                                │ │
│  │                                                                        │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                              ↓                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                         GENERATE ACTIONS                               │ │
│  │  • new_price (from margin tiers)                                       │ │
│  │  • sku_discount_flag (ADD / REMOVE / KEEP / NO)                        │ │
│  │  • qd_discount_flag (ADD / REMOVE_T3 / REMOVE_T2 / REMOVE_T1 / KEEP)   │ │
│  │  • tier_with_problem (T3 / T2 / T1 / None)                             │ │
│  │  • new_cart_rule                                                       │ │
│  │  • action_reason                                                       │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                              ↓                                               │
│                    pricing_actions_{timestamp}.xlsx                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Input/Output

### Input
- `pricing_with_discount.xlsx` - Output from data_extraction.ipynb

### Output Columns
| Column | Description |
|--------|-------------|
| `new_price` | Recommended target price |
| `sku_discount_flag` | ADD / REMOVE / KEEP / NO |
| `qd_discount_flag` | REMOVE_T3 / REMOVE_T2 / REMOVE_T1 / KEEP / NO |
| `tier_with_problem` | Which QD tier to remove (T3, T2, T1, or None) |
| `new_cart_rule` | New cart rule value |
| `action_reason` | Explanation of why this action was taken |
| `hit_price_floor` | True if price hit commercial_min |

---

## Condition Logic (Priority Order)

**Important: No actions taken for SKUs with stocks = 0**

### Condition 1: Zero Demand + Stock > 0

**Trigger:** `zero_demand == 1` AND `stocks > 0`

| Action | Value |
|--------|-------|
| Price | **-2 steps** down in tiers |
| SKU Discount | **ADD** |
| Quantity Discount | **NO** (don't add) |
| Cart Rule | **+25%** (open) |

```
Zero demand SKUs with stock need aggressive price reduction to move inventory.
Don't add QD because there's no demand to incentivize bulk purchases.
Open cart rules to allow any purchases that might happen.
```

---

### Condition 2: Star Performer / Over Achiever (NOT zero demand) + Stock > 0

**Trigger:** `combined_status` in ['Star Performer', 'Over Achiever'] AND `zero_demand == 0` AND `stocks > 0`

| Scenario | Price | SKU Discount | QD | Cart Rule |
|----------|-------|--------------|-----|-----------|
| SKU disc contribution > 50% | Keep | **REMOVE** | Keep | -25% |
| QD contribution > 50% | Keep | Keep | **REMOVE highest tier** | -25% |
| Both contributions < 50% | **+1 step** | Keep | Keep | -25% |

```
High performers don't need discounts to sell. If discounts are driving >50% of sales:
- For SKU discount: Remove it (product sells well without it)
- For QD: Remove the HIGHEST tier first (T3 → T2 → T1)

Only increase price if BOTH discount contributions are below 50%.
Always restrict cart to protect stock for high-demand items.
```

**QD Tier Removal Priority:**
1. If T3 exists and has contribution → Remove T3
2. If T2 is max tier → Remove T2
3. If T1 is max tier → Remove T1

---

### Condition 3: On Track + Stock > 0 (NOT zero demand)

**Trigger:** `combined_status == 'On Track'` AND `stocks > 0` AND `zero_demand == 0`

| Action | Value |
|--------|-------|
| Price | **+1 step** up in tiers |
| SKU Discount | **KEEP** (if exists) |
| Quantity Discount | **KEEP** (if exists) |
| Cart Rule | **KEEP** (no change) |

```
On Track SKUs are performing well with current setup.
Increase price slightly to capture more margin.
Keep existing discounts and cart rules - don't fix what's not broken.
```

---

### Condition 4: Struggling / Underperforming / Critical + Stock > 0 (NOT zero demand)

**Trigger:** `combined_status` in ['Struggling', 'Underperforming', 'Critical'] AND `stocks > 0` AND `zero_demand == 0`

| Status | Price | SKU Discount | QD | Cart Rule |
|--------|-------|--------------|-----|-----------|
| Struggling | **-1 step** | ADD | KEEP | +25% |
| Underperforming | **-1 step** | ADD | KEEP | +25% |
| **Critical** | **-2 steps** | ADD | KEEP | +25% |

```
Underperforming SKUs need price reduction and discounts to boost sales.
- Struggling/Underperforming: Reduce price by 1 step
- Critical: More aggressive - reduce price by 2 steps
Add SKU discount to all.
Don't add new QD (keep existing if any) - focus on base demand first.
Open cart rules to encourage larger purchases.
```

---

### Condition 5: No Data + Stock > 0 (NOT zero demand)

**Trigger:** `combined_status == 'no_data'` AND `stocks > 0` AND `zero_demand == 0`

| Action | Value |
|--------|-------|
| Price | **-2 steps** down in tiers |
| SKU Discount | **ADD** |
| Quantity Discount | **NO** |
| Cart Rule | **+25%** (open) |

```
SKUs with no performance data and stock need aggressive action.
Assume they're struggling and apply similar treatment to zero demand.
Don't add QD since we don't know demand patterns yet.
```

---

## Price Tier System

### All Tiers Are Margins

Both market data and internal data contain **margins**, not prices. Prices are calculated as:

```
price = WAC / (1 - margin)
```

### Tier Priority

1. **Market Margins** (if available):
   - `below_market` → `market_min` → `market_25` → `market_50` → `market_75` → `market_max` → `above_market`

2. **Internal Margins** (extend range if needed):
   - `margin_tier_below` → `margin_tier_1` → ... → `margin_tier_5` → `margin_tier_above_1` → `margin_tier_above_2`

3. **Markup Fallback** (if no margin data):
   - Apply ±3% per step to current price

### Commercial Minimum Floor

If the calculated new price goes **below `commercial_min_price`**:
1. Set price to `commercial_min_price`
2. Mark `hit_price_floor = True`
3. **ADD the SKU to SKU discount** (to achieve the effective lower price via discount)

---

## Cart Rule Logic

### Calculation

```python
# Open (+25%)
change = max(2, current_cart * 0.25)
new_cart = current_cart + change

# Restrict (-25%)
change = max(2, current_cart * 0.25)
new_cart = current_cart - change
```

### Constraints
- **Minimum change:** 2 units
- **Minimum value:** 2 units

---

## QD Tier Problem Detection

For Star/Over Achiever SKUs with QD contribution > 50%, remove tiers from **highest to lowest**:

```
T3 (highest) → T2 → T1 (lowest)
```

The `tier_with_problem` column indicates which tier should be removed.

---

## Decision Flow Diagram

```
                              ┌─────────────────┐
                              │   START: Read   │
                              │   Input Data    │
                              └────────┬────────┘
                                       │
                                       ▼
                         ┌─────────────────────────┐
                         │  Zero Demand + Stock>0? │
                         └────────────┬────────────┘
                                      │
                    ┌─────────────────┴─────────────────┐
                    │ YES                               │ NO
                    ▼                                   ▼
          ┌─────────────────┐              ┌───────────────────────────────┐
          │ Price: -2 steps │              │ Star/Over Achiever + Stock>0? │
          │ SKU Disc: ADD   │              └────────────┬──────────────────┘
          │ QD: NO          │                           │
          │ Cart: +25%      │            ┌──────────────┴──────────────┐
          └─────────────────┘            │ YES                         │ NO
                                         ▼                             ▼
                              ┌──────────────────────┐    ┌─────────────────────┐
                              │ Check Contributions: │    │ On Track + Stock>0? │
                              │ • SKU disc >50%?     │    └──────────┬──────────┘
                              │ • QD >50%?           │               │
                              └──────────┬───────────┘    ┌──────────┴──────────┐
                                         │                │ YES                 │ NO
                    ┌────────────────────┼────────────┐   ▼                     ▼
                    │                    │            │   ┌─────────────┐  ┌────────────────────┐
                    ▼                    ▼            ▼   │ Price: +1   │  │ Struggling/Under-  │
          ┌─────────────────┐  ┌─────────────┐  ┌─────┐  │ SKU: KEEP   │  │ performing/Critical│
          │ SKU disc >50%:  │  │ QD >50%:    │  │Both │  │ QD: KEEP    │  │ + Stock>0?         │
          │ REMOVE SKU disc │  │ REMOVE      │  │<50% │  │ Cart: KEEP  │  └─────────┬──────────┘
          └─────────────────┘  │ highest tier│  │     │  └─────────────┘            │
                               └─────────────┘  │     │            ┌────────────────┴────────┐
                                                ▼     │            │ YES                     │ NO
                                         ┌──────────┐ │            ▼                         ▼
                                         │ Price:   │ │  ┌─────────────────┐     ┌──────────────────┐
                                         │ +1 step  │ │  │ Strug/Under: -1 │     │ No Data+Stock>0? │
                                         └──────────┘ │  │ Critical: -2    │     └────────┬─────────┘
                                                      │  │ SKU Disc: ADD   │              │
                              All paths: Cart -25%    │  │ Cart: +25%      │    ┌─────────┴─────────┐
                                                      │  └─────────────────┘    │ YES               │ NO
                                                      │                         ▼                   ▼
                                                      │              ┌─────────────────┐  ┌──────────────┐
                                                      │              │ Price: -2 steps │  │ NO ACTION    │
                                                      │              │ SKU Disc: ADD   │  │ (No stock or │
                                                      │              │ QD: NO          │  │ other case)  │
                                                      │              │ Cart: +25%      │  └──────────────┘
                                                      │              └─────────────────┘
                                                      │
                                                      ▼
                                              ┌───────────────┐
                                              │     END       │
                                              └───────────────┘
```

---

## Output Files

| File | Description |
|------|-------------|
| `pricing_actions_{timestamp}.xlsx` | Full action recommendations |

---

## Configuration Constants

```python
# Cart Rule Settings
MIN_CART_RULE = 2              # Minimum cart rule value
MIN_CART_CHANGE = 2            # Minimum change amount
CART_INCREASE_PCT = 0.25       # 25% increase (open)
CART_DECREASE_PCT = 0.25       # 25% decrease (restrict)

# Contribution Threshold
CONTRIBUTION_THRESHOLD = 0.50  # 50% threshold for discount removal
```

---

## Version History

| Date | Version | Changes |
|------|---------|---------|
| 2026-01-19 | 2.0 | Complete rewrite with new condition-based logic |
