"""
Shared constants for the MaxAB Egypt pricing system.

Import from here instead of hardcoding in individual notebooks.
"""

REGION = "Egypt"
TIMEZONE = "Africa/Cairo"

# Warehouse mapping: (region, warehouse_name, warehouse_id, cohort_id)
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

COHORT_IDS = [700, 701, 702, 703, 704, 1123, 1124, 1125, 1126]

# Parent→child warehouse pairs for stock rollup
PARENT_CHILD_WAREHOUSES = [(236, 343), (1, 467), (962, 343)]

# Warehouses excluded from queries
EXCLUDED_WAREHOUSE_IDS = [6, 9, 10]

# All active warehouse IDs (for stock/price queries)
ALL_WAREHOUSE_IDS = [1, 8, 170, 236, 337, 339, 401, 501, 632, 703, 797, 962, 343, 467]

# Sales channels included in queries
SALES_CHANNELS = ['telesales', 'retailer']

# Order statuses excluded from sales queries
EXCLUDED_ORDER_STATUSES = [7, 12]

# Region → cohort mapping (used by market data module)
REGION_COHORT_MAP = {
    'Cairo': 700,
    'Giza': 701,
    'Alexandria': 702,
    'Delta West': 703,
    'Delta East': 704,
    'Upper Egypt - Menya': 1123,
    'Upper Egypt - Assiut': 1124,
    'Upper Egypt - Sohag': 1125,
    'Upper Egypt - Beni Suef': 1126,
}

# QD handler warehouse → tag mapping
QD_WAREHOUSE_TAG_MAPPING = {
    501: 3301, 401: 3302, 236: 3303, 1: 3304,
    337: 3305, 339: 3306, 8: 3307, 170: 3308,
    703: 3309, 632: 3310, 797: 3311, 962: 3312,
}

# Snowflake warehouse used for compute
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
