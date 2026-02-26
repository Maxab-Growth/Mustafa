# =============================================================================
# PRICING HELPERS
# =============================================================================
# Shared functions for all pricing modules
# =============================================================================

import pandas as pd
import numpy as np
import sys
import os

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.pricing_config import (
    ABC_MARKET_PERCENTILES, ABC_MARGIN_PERCENTILES, ABC_CART_STD_MULTIPLIERS,
    MIN_CART_RULE, MAX_CART_RULE, MIN_CART_CHANGE, CART_ADJUST_PCT,
    MIN_PRICE_REDUCTION_PCT, CART_TOO_OPEN_STD,
    STATUS_BELOW_ON_TRACK, STATUS_ABOVE_ON_TRACK, STATUS_ON_TRACK,
    GROWING_THRESHOLD, DROPPING_THRESHOLD
)


# =============================================================================
# STATUS CLASSIFICATION
# =============================================================================

def is_below_on_track(status):
    """Check if status is below On Track (struggling/critical/etc)."""
    return status in STATUS_BELOW_ON_TRACK


def is_above_on_track(status):
    """Check if status is above On Track (over achiever/star performer)."""
    return status in STATUS_ABOVE_ON_TRACK


def is_on_track(status):
    """Check if status is On Track."""
    return status in STATUS_ON_TRACK


def get_status_category(status):
    """
    Classify status as 'below', 'on_track', or 'above'.
    """
    if is_below_on_track(status):
        return 'below'
    elif is_above_on_track(status):
        return 'above'
    elif is_on_track(status):
        return 'on_track'
    else:
        return 'unknown'


# =============================================================================
# PRICE TIER FUNCTIONS
# =============================================================================

def build_price_tiers(row):
    """
    Build sorted list of price tiers from margins.
    Uses market margins first, then extends with internal margins.
    
    Returns: List of (price, tier_name, margin) tuples sorted low to high
    """
    wac = row.get('wac_p', 0)
    
    if pd.isna(wac) or wac <= 0:
        return []
    
    tiers = []
    
    # Market margins (primary)
    market_cols = [
        ('below_market', 'below_market'),
        ('market_min', 'market_min'),
        ('market_25', 'market_25'),
        ('market_50', 'market_50'),
        ('market_75', 'market_75'),
        ('market_max', 'market_max'),
        ('above_market', 'above_market')
    ]
    
    # Internal margins (extend range if needed)
    internal_cols = [
        ('margin_tier_below', 'internal_below'),
        ('margin_tier_1', 'internal_1'),
        ('margin_tier_2', 'internal_2'),
        ('margin_tier_3', 'internal_3'),
        ('margin_tier_4', 'internal_4'),
        ('margin_tier_5', 'internal_5'),
        ('margin_tier_above_1', 'internal_above_1'),
        ('margin_tier_above_2', 'internal_above_2')
    ]
    
    # Collect all valid margins
    all_cols = market_cols + internal_cols
    
    for col, name in all_cols:
        margin = row.get(col)
        if pd.notna(margin) and 0 <= margin < 1:
            price = wac / (1 - margin)
            tiers.append((price, name, margin))
    
    # Sort by price (low to high) and remove duplicates
    tiers = sorted(set(tiers), key=lambda x: x[0])
    
    return tiers


def find_current_tier_index(current_price, tiers):
    """Find which tier index the current price is closest to."""
    if not tiers:
        return -1
    
    for i, (price, name, margin) in enumerate(tiers):
        if current_price <= price:
            return i
    
    return len(tiers) - 1


def get_price_at_steps(row, steps):
    """
    Get price after moving N steps in tiers.
    Negative steps = lower price, Positive steps = higher price
    
    Returns: (new_price, tier_name, hit_floor)
    """
    current_price = row.get('current_price', 0)
    commercial_min = row.get('commercial_min_price', 0) or 0
    wac = row.get('wac_p', 0)
    
    if pd.isna(current_price) or current_price <= 0:
        return current_price, 'invalid', False
    
    tiers = build_price_tiers(row)
    
    if not tiers:
        # No tiers - use markup fallback based on 10% of current margin per step
        if pd.notna(wac) and wac > 0 and current_price > wac:
            current_margin = (current_price - wac) / current_price
            markup_per_step = 0.1 * current_margin
        else:
            markup_per_step = 0.03  # Fallback to 3% if no valid margin
        
        markup = steps * markup_per_step
        new_price = current_price * (1 + markup)
        return round(new_price, 2), f'markup_{markup*100:.1f}%', False
    
    # Find current position
    current_idx = find_current_tier_index(current_price, tiers)
    
    # Calculate new position
    new_idx = current_idx + steps
    new_idx = max(0, min(len(tiers) - 1, new_idx))
    
    new_price, tier_name, _ = tiers[new_idx]
    
    # Enforce minimum price reduction of 0.25% when reducing price
    if steps < 0 and new_price >= current_price:
        min_reduced_price = current_price * (1 - MIN_PRICE_REDUCTION_PCT)
        if new_price > min_reduced_price:
            new_price = min_reduced_price
            tier_name = f'min_reduction_{MIN_PRICE_REDUCTION_PCT*100:.2f}%'
    elif steps < 0:
        actual_reduction_pct = (current_price - new_price) / current_price
        if actual_reduction_pct < MIN_PRICE_REDUCTION_PCT:
            min_reduced_price = current_price * (1 - MIN_PRICE_REDUCTION_PCT)
            new_price = min_reduced_price
            tier_name = f'min_reduction_{MIN_PRICE_REDUCTION_PCT*100:.2f}%'
    
    # Check commercial minimum floor
    hit_floor = False
    if commercial_min > 0 and new_price < commercial_min:
        new_price = commercial_min
        tier_name = 'commercial_min'
        hit_floor = True
    
    return round(new_price, 2), tier_name, hit_floor


# =============================================================================
# TARGET PRICE BY ABC CLASS (Module 2)
# =============================================================================

def get_target_price_by_class(row, abc_class):
    """
    Get target price based on ABC class.
    
    Priority:
    1. Market data exists → use market percentile
    2. No market data → use margin tier percentage
    3. No data at all → return None (will use category average)
    
    Returns: (target_price, source) or (None, 'no_data')
    """
    wac = row.get('wac_p', 0)
    
    if pd.isna(wac) or wac <= 0:
        return None, 'no_wac'
    
    # Try market data first
    market_percentile = ABC_MARKET_PERCENTILES.get(abc_class, 50)
    market_col = f'market_{market_percentile}'
    market_margin = row.get(market_col)
    
    if pd.notna(market_margin) and 0 <= market_margin < 1:
        target_price = wac / (1 - market_margin)
        return round(target_price, 2), f'market_{market_percentile}'
    
    # Fall back to margin tiers
    margin_pct = ABC_MARGIN_PERCENTILES.get(abc_class, 75) / 100
    
    # Get min and max margin from internal tiers
    min_margin = row.get('margin_tier_1')
    max_margin = row.get('margin_tier_5')
    
    # Try to find min/max from available tiers
    if pd.isna(min_margin):
        for col in ['margin_tier_1', 'margin_tier_2', 'margin_tier_3']:
            val = row.get(col)
            if pd.notna(val):
                min_margin = val
                break
    
    if pd.isna(max_margin):
        for col in ['margin_tier_5', 'margin_tier_4', 'margin_tier_3']:
            val = row.get(col)
            if pd.notna(val):
                max_margin = val
                break
    
    if pd.notna(min_margin) and pd.notna(max_margin) and max_margin > min_margin:
        # Calculate target margin as percentage between min and max
        target_margin = min_margin + margin_pct * (max_margin - min_margin)
        target_price = wac / (1 - target_margin)
        return round(target_price, 2), f'margin_tier_{int(margin_pct*100)}%'
    
    return None, 'no_data'


# =============================================================================
# CART RULE FUNCTIONS
# =============================================================================

def adjust_cart_rule(current_cart, direction, normal_refill=0, refill_stddev=0, abc_class='C'):
    """
    Adjust cart rule based on direction and normal_refill data.
    
    direction: 'open' (increase) or 'restrict' (decrease) or 'keep'
    
    OPEN (increase) rules:
    - If current < normal_refill + stddev → set to threshold
    - If current >= threshold → increase by 20%
    - If already above 150 → don't increase
    
    RESTRICT (decrease) rules:
    - Decrease by 20%
    - Minimum is 2 units (absolute floor)
    
    Returns: new cart rule value
    """
    current_cart = current_cart if pd.notna(current_cart) and current_cart > 0 else 999
    normal_refill = normal_refill if pd.notna(normal_refill) and normal_refill > 0 else 0
    refill_stddev = refill_stddev if pd.notna(refill_stddev) else 0
    
    # Target threshold = normal_refill + stddev
    target_threshold = normal_refill + refill_stddev if normal_refill > 0 else 0
    
    if direction == 'keep':
        return int(current_cart)
    
    if direction == 'open':
        # Don't increase if already above MAX_CART_RULE
        if current_cart >= MAX_CART_RULE:
            return int(current_cart)
        
        # If below threshold, jump to threshold
        if target_threshold > 0 and current_cart < target_threshold:
            new_cart = target_threshold
        else:
            # Already at or above threshold, increase by 20%
            change = max(MIN_CART_CHANGE, int(current_cart * CART_ADJUST_PCT))
            new_cart = current_cart + change
        
        # Cap at MAX_CART_RULE
        return min(MAX_CART_RULE, int(new_cart))
    
    elif direction == 'restrict':
        # Decrease by 20%
        change = max(MIN_CART_CHANGE, int(current_cart * CART_ADJUST_PCT))
        new_cart = current_cart - change
        
        # Minimum is MIN_CART_RULE (2 units)
        return max(MIN_CART_RULE, int(new_cart))
    
    return int(current_cart)


def get_initial_cart_rule(current_cart, normal_refill, refill_stddev, abc_class):
    """
    Get initial cart rule for Module 2 (daily reset).
    
    If current_cart < normal_refill:
    - A class → normal_refill + 1×std
    - B class → normal_refill + 2×std
    - C class → normal_refill + 5×std
    
    Returns: new cart rule value
    """
    current_cart = current_cart if pd.notna(current_cart) and current_cart > 0 else 999
    normal_refill = normal_refill if pd.notna(normal_refill) and normal_refill > 0 else 0
    refill_stddev = refill_stddev if pd.notna(refill_stddev) else 0
    
    if normal_refill == 0:
        return int(current_cart)
    
    # Only adjust if current_cart is below normal_refill
    if current_cart < normal_refill:
        std_multiplier = ABC_CART_STD_MULTIPLIERS.get(abc_class, 5)
        new_cart = normal_refill + (std_multiplier * refill_stddev)
        return min(MAX_CART_RULE, max(MIN_CART_RULE, int(new_cart)))
    
    return int(current_cart)


def is_cart_too_open(current_cart, normal_refill, refill_stddev):
    """
    Check if cart rule is too open (> normal_refill + 10×std).
    Used in Module 3 GROWING scenario.
    """
    if pd.isna(normal_refill) or normal_refill <= 0:
        return False
    
    refill_stddev = refill_stddev if pd.notna(refill_stddev) else 0
    threshold = normal_refill + (CART_TOO_OPEN_STD * refill_stddev)
    
    return current_cart > threshold


# =============================================================================
# UTH (UP-TILL-HOUR) CALCULATIONS (Module 3)
# =============================================================================

def calculate_uth_targets(p80_daily_qty, p70_daily_retailers, hourly_qty_pct, hourly_retailer_pct):
    """
    Calculate UTH targets based on historical patterns.
    
    Args:
        p80_daily_qty: P80 benchmark for daily quantity
        p70_daily_retailers: P70 benchmark for daily retailers
        hourly_qty_pct: % of daily qty typically sold by current hour
        hourly_retailer_pct: % of daily retailers typically active by current hour
    
    Returns: (qty_target_uth, retailer_target_uth)
    """
    qty_target = (p80_daily_qty or 0) * (hourly_qty_pct or 0)
    retailer_target = (p70_daily_retailers or 0) * (hourly_retailer_pct or 0)
    
    return qty_target, retailer_target


def get_uth_status(actual_qty, target_qty):
    """
    Determine UTH status based on actual vs target.
    
    Returns: 'growing', 'on_track', or 'dropping'
    """
    if target_qty <= 0:
        return 'no_target'
    
    ratio = actual_qty / target_qty
    
    if ratio > GROWING_THRESHOLD:
        return 'growing'
    elif ratio < DROPPING_THRESHOLD:
        return 'dropping'
    else:
        return 'on_track'


# =============================================================================
# QD TIER ANALYSIS
# =============================================================================

def get_qd_tier_to_remove(row):
    """
    Determine which QD tier to remove (highest first: T3 → T2 → T1).
    
    Returns: (tier_to_remove, qd_cntrb)
        tier_to_remove: 'T3', 'T2', 'T1', or None
        qd_cntrb: Total QD contribution
    """
    # Try UTH contributions first (for Module 3), then yesterday contributions
    t1_cntrb = row.get('t1_cntrb_uth', row.get('yesterday_t1_cntrb', 0)) or 0
    t2_cntrb = row.get('t2_cntrb_uth', row.get('yesterday_t2_cntrb', 0)) or 0
    t3_cntrb = row.get('t3_cntrb_uth', row.get('yesterday_t3_cntrb', 0)) or 0
    
    qd_cntrb = row.get('qty_disc_cntrb_uth', row.get('yesterday_qty_disc_cntrb', 0)) or 0
    
    # Find highest contributing tier
    tiers = [('T3', t3_cntrb), ('T2', t2_cntrb), ('T1', t1_cntrb)]
    
    # Sort by contribution descending
    tiers_with_value = [(t, c) for t, c in tiers if c > 0]
    
    if tiers_with_value:
        tiers_with_value.sort(key=lambda x: x[1], reverse=True)
        return tiers_with_value[0][0], qd_cntrb
    
    return None, 0


def get_highest_contributing_tier(t1_cntrb, t2_cntrb, t3_cntrb):
    """
    Get the tier with highest contribution for removal.
    Returns: 'T1', 'T2', 'T3', or None
    """
    tiers = {'T1': t1_cntrb or 0, 'T2': t2_cntrb or 0, 'T3': t3_cntrb or 0}
    
    if all(v == 0 for v in tiers.values()):
        return None
    
    return max(tiers, key=tiers.get)


# =============================================================================
# MARGIN CALCULATIONS
# =============================================================================

def calculate_margin(price, wac):
    """Calculate margin given price and WAC."""
    if pd.isna(price) or pd.isna(wac) or price <= 0:
        return None
    return (price - wac) / price


def calculate_price_from_margin(wac, margin):
    """Calculate price given WAC and margin."""
    if pd.isna(wac) or pd.isna(margin) or margin >= 1:
        return None
    return wac / (1 - margin)


print("✅ Pricing helpers loaded")

