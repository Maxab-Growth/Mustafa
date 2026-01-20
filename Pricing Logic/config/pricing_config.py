# =============================================================================
# PRICING MODEL CONFIGURATION
# =============================================================================
# Central configuration for all pricing modules
# =============================================================================

from datetime import datetime

# =============================================================================
# MODULE RUN TIMES
# =============================================================================
MODULE_2_RUN_TIME = "06:00"  # Initial Price Push (Cairo time)
MODULE_3_RUN_TIMES = ["12:00", "15:00", "18:00", "21:00", "00:00"]  # Periodic Actions

# =============================================================================
# ABC CLASS PRICING SETTINGS
# =============================================================================

# Market percentiles by ABC class (when market data exists)
# A-class gets lower percentile (more aggressive pricing)
ABC_MARKET_PERCENTILES = {
    'A': 25,   # market_25 price
    'B': 50,   # market_50 price  
    'C': 75    # market_75 price
}

# Margin tier percentages by ABC class (when no market data)
# This is the % point within the margin range (min_margin to max_margin)
ABC_MARGIN_PERCENTILES = {
    'A': 50,   # 50% of margin range
    'B': 75,   # 75% of margin range
    'C': 90    # 90% of margin range
}

# Cart rule standard deviation multipliers by ABC class
# Used when cart < normal_refill in Module 2
ABC_CART_STD_MULTIPLIERS = {
    'A': 1,    # normal_refill + 1×std
    'B': 2,    # normal_refill + 2×std
    'C': 5     # normal_refill + 5×std
}

# =============================================================================
# CART RULE SETTINGS
# =============================================================================
MIN_CART_RULE = 2              # Absolute minimum cart rule (units)
MAX_CART_RULE = 150            # Don't increase above this
MIN_CART_CHANGE = 2            # Minimum change amount
CART_ADJUST_PCT = 0.20         # 20% adjustment for open/restrict
CART_TOO_OPEN_STD = 10         # Cart > normal_refill + 10×std is "too open"

# =============================================================================
# PRICE SETTINGS
# =============================================================================
MIN_PRICE_REDUCTION_PCT = 0.0025  # 0.25% minimum price reduction

# =============================================================================
# PERFORMANCE THRESHOLDS
# =============================================================================
ON_TRACK_THRESHOLD = 0.10      # ±10% of target = On Track
GROWING_THRESHOLD = 1.10       # >110% of target = Growing
DROPPING_THRESHOLD = 0.90      # <90% of target = Dropping

# Contribution threshold (values already in % format, e.g., 50 = 50%)
CONTRIBUTION_THRESHOLD = 50

# =============================================================================
# STATUS HIERARCHY
# =============================================================================
# For determining "below" or "above" On Track
STATUS_BELOW_ON_TRACK = ['No Data', 'Critical', 'Struggling', 'Underperforming']
STATUS_ABOVE_ON_TRACK = ['Over Achiever', 'Star Performer']
STATUS_ON_TRACK = ['On Track']

# =============================================================================
# DATA EXTRACTION SETTINGS
# =============================================================================
P80_BENCHMARK_DAYS = 240       # Days for P80 qty benchmark (was 180, now 240 per existing code)
P70_RETAILER_DAYS = 240        # Days for P70 retailer benchmark
HOURLY_PATTERN_DAYS = 120      # 4 months for hourly distribution patterns

# =============================================================================
# TIMEZONE
# =============================================================================
TIMEZONE = 'UTC'
LOCAL_TIMEZONE = 'Africa/Cairo'

# =============================================================================
# FILE PATHS
# =============================================================================
INPUT_FILE = 'pricing_with_discount.xlsx'
OUTPUT_DIR = 'outputs'

def get_output_filename(module_name):
    """Generate timestamped output filename."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    return f"{module_name}_{timestamp}.xlsx"

# =============================================================================
# SNOWFLAKE SETTINGS
# =============================================================================
WAREHOUSE = "COMPUTE_WH"

# Warehouse Mapping: (region, warehouse_name, warehouse_id, cohort_id)
WAREHOUSE_MAPPING = [
    ('Cairo', 'Mostorod', 1, 700),
    ('Giza', 'Barageel', 236, 701),
    ('Giza', 'Sakkarah', 962, 701),
    ('Delta West', 'El-Mahala', 337, 703),
    ('Delta West', 'Tanta', 8, 703),
    ('Delta East', 'Mansoura FC', 339, 704),
    ('Delta East', 'Sharqya', 170, 704),
    ('Upper Egypt', 'Assiut FC', 501, 1124),
    ('Upper Egypt', 'Bani sweif', 401, 1126),
    ('Upper Egypt', 'Menya Samalot', 703, 1123),
    ('Upper Egypt', 'Sohag', 632, 1125),
    ('Alexandria', 'Khorshed Alex', 797, 702),
]

# Get list of warehouse IDs
WAREHOUSE_IDS = [wh[2] for wh in WAREHOUSE_MAPPING]

print("✅ Pricing configuration loaded")

