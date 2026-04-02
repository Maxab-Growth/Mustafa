# SKU Mapping Pipeline

## Purpose

Automatically maps MaxAB product SKUs to competitor/scraped product listings from four external sources (Cartona, Tawfeer, Speed, Talabia). This mapping feeds the Market Data Module with competitor price references used across the entire pricing system. The pipeline validates existing manual mappings, runs a fuzzy-matching algorithm on unmapped products, and combines both into a unified mapping table.

---

## Pipeline Flow

```mermaid
flowchart TD
    START[Start] --> LOAD[Load Data from Snowflake]

    LOAD --> L1["MaxAB Products\n~3,800 unique products\nwith prices, stock, NMV"]
    LOAD --> L2["Scraped Data\n~7,500 rows from 4 apps\nCartona, Tawfeer, Speed, Talabia"]
    LOAD --> L3["Existing Manual Mapping\n~7,200 rows from\ncompetitors_mapping_fixed"]

    L1 --> PREP[Preprocessing]
    L2 --> PREP
    L3 --> PREP
    PREP --> PREP_D["Arabic normalization\nUnit type canonicalization\nBrand/category vocabulary\nAWN synonym expansion"]

    PREP_D --> STEP1["Step 1: Validate\nExisting Mapping"]
    STEP1 --> V_CHK{"For each existing pair:\nsize check → variant check\n→ text similarity ≥ 35?"}
    V_CHK -- Pass --> VALIDATED["Validated\n~2,500 rows"]
    V_CHK -- Fail --> REJECTED["Rejected\n~4,600 rows"]

    VALIDATED --> STEP2["Step 2: Algorithm\non Unmapped"]
    REJECTED --> STEP2
    STEP2 --> ALG["For each unmapped scraped product:\n1. Brand matching\n2. Candidate retrieval\n3. Filter: category, size, variant\n4. Score: text + brand + size + price\n5. Unit-level matching"]
    ALG --> ALGO_OUT["Algorithm Results\n~1,400 matched\n~3,400 result rows"]

    VALIDATED --> STEP3["Step 3: Combine"]
    ALGO_OUT --> STEP3
    STEP3 --> COMBINED["Combined Mapping\n~2,000 unique MaxAB products\n~52% coverage"]

    COMBINED --> EXPORT[Export]
    EXPORT --> E1["mapping_combined.xlsx"]
    EXPORT --> E2["mapping_algorithm_results.xlsx"]
    EXPORT --> E3["mapping_existing_validated.xlsx"]
    EXPORT --> E4["mapping_existing_rejected.xlsx"]
    EXPORT --> E5["mapping_unmatched.xlsx"]
```

---

## Matching Algorithm Detail

```mermaid
flowchart TD
    INPUT[Scraped Product] --> BRAND{"Brand Detection"}

    BRAND -- "Tawfeer\n(has brand field)" --> BT["match_brand_tawfeer\nExact → subset → fuzzy ≥90"]
    BRAND -- "Other apps\n(brand in product name)" --> BF["find_brand_in_text\nScan first tokens against\nMaxAB brand list"]

    BT --> CANDS[Candidate MaxAB Products\nAll products sharing matched brand]
    BF --> CANDS

    CANDS --> FILTER["Filter Candidates"]
    FILTER --> F1{"Category compatible?\nVocabulary + AWN synonyms"}
    F1 -- No & sim < 90 --> SKIP[Skip]
    F1 -- Yes --> F2{"Size compatible?\nWeight/volume/count\n±15% tolerance"}
    F2 -- Mismatch --> SKIP
    F2 -- OK --> F3{"Variant conflict?\nFlavor/color/type mismatch"}
    F3 -- Conflict --> SKIP
    F3 -- OK --> SCORE

    SCORE["Score Candidates"]
    SCORE --> S1["text_sim: token_sort + token_set\n+ partial ratio (weighted)"]
    SCORE --> S2["brand_sim × 0.05–0.15"]
    SCORE --> S3["+10 bonus if size confirmed"]

    S1 --> UNIT["Unit-Level Matching"]
    S2 --> UNIT
    S3 --> UNIT

    UNIT --> U1["Unit type match: +30"]
    UNIT --> U2["Price compatibility\n≤20% diff: +0–30\n>30% diff: reject"]

    U1 --> FINAL["combined_score =\ntext + brand + size + unit + price"]
    U2 --> FINAL
    FINAL --> BEST["Return top match\n+ alternatives within 90%"]
```

---

## Arabic NLP Processing

The pipeline includes a custom Arabic NLP layer for handling product name matching:

| Component | Description |
|-----------|-------------|
| **Diacritics removal** | Strips all Arabic diacritical marks (tashkeel) |
| **Alef normalization** | Unifies alef variants (أ, إ, آ, ٱ) → ا |
| **Taa marbuta / Alef maqsura** | ة → ه, ى → ي for consistent matching |
| **Eastern digits** | ٠-٩ converted to 0-9 |
| **Measurement normalization** | جرام/غرام/g → جم, كيلو/kg → كجم, ملل/ml → مل, etc. |
| **Size extraction** | Parses `(\d+)\s*(unit)` patterns; normalizes to base units (g, ml, mm) |
| **Count extraction** | Detects quantity × unit patterns (e.g., "12 قطعة", "6 رول") |
| **AWN synonyms** | Arabic WordNet expansion for category vocabulary matching |
| **Al-prefix stripping** | Optional removal of ال prefix for token comparison |

---

## Key Functions

| Function | Description |
|----------|-------------|
| `normalize_arabic` | Full Arabic text normalization pipeline |
| `extract_size_info` / `extract_count_info` | Parse weight/volume/count from product names |
| `sizes_compatible` | Compare two products' sizes with ±15% tolerance |
| `variant_conflict` | Detect flavor/color/type mismatches (e.g., "strawberry" vs "mango") |
| `category_compatible` | Check if scraped product fits MaxAB category (vocabulary + AWN) |
| `descriptive_overlap` | Verify shared descriptive tokens beyond brand/noise words |
| `text_sim` | Max of token_sort_ratio, token_set_ratio × 0.95, partial_ratio × 0.85 |
| `find_brand_in_text` | Detect MaxAB brand names within scraped product text |
| `match_brand_tawfeer` | Brand matching for Tawfeer (has separate brand field) |
| `price_compat` | Multi-strategy price comparison (direct, per-unit, same-qty, blind) |
| `validate_existing_match` | Validate a manual mapping pair against size/variant/text filters |
| `get_candidates` | Retrieve and score candidate MaxAB products for a scraped item |
| `match_units` | Match at packing-unit level with unit type and price scoring |
| `map_one` | Full matching pipeline for a single scraped product |

---

## Inputs / Outputs

### Inputs

| Source | Query File | Description |
|--------|-----------|-------------|
| MaxAB products | `current_sku_data.sql` | Active products with stock or NMV (cohort 700 prices, stocks, 120d sales) |
| Scraped data | `raw_scraped_data_latest.sql` | Last 4 days of competitor prices from `raw_scraped_data` |
| Existing mapping | `existing_mapping_query.sql` | Manual mapping from `competitors_mapping_fixed` |

### Outputs

| File | Contents |
|------|----------|
| `mapping_combined.xlsx` | All validated + algorithm matches (primary output) |
| `mapping_algorithm_results.xlsx` | Algorithm matches only, with scores and price comparison |
| `mapping_existing_validated.xlsx` | Existing manual mappings that passed validation |
| `mapping_existing_rejected.xlsx` | Existing manual mappings that failed validation (with reasons) |
| `mapping_unmatched.xlsx` | Scraped products with no match found |

---

## Matching Thresholds

| Parameter | Value | Description |
|-----------|-------|-------------|
| `SOFT_THRESHOLD` | 90 | Text similarity below this requires additional checks (category, descriptive overlap) |
| `min_ts` (in `map_one`) | 45 | Minimum text similarity to consider a candidate |
| `PRICE_HARD_MAX` | 0.30 (30%) | Maximum price difference allowed |
| Price compatible | ≤ 0.20 (20%) | Price difference considered "compatible" |
| Size tolerance | 0.15 (15%) | Allowed size/count deviation |
| Existing mapping text sim | ≥ 35 | Looser threshold for human-validated pairs |
| Brand fuzzy match | ≥ 90 | Minimum score for fuzzy brand matching |
| Category token fuzzy | ≥ 75 | Minimum score for category vocabulary token matching |
| Variant words | ~130 terms | Flavors, colors, types, subtypes that trigger conflict detection |

---

## Competitor Sources

| Source | Has Brand Field | Has Quantity | Notes |
|--------|----------------|--------------|-------|
| **Cartona** | No (in product name) | No | Brand detected from product text; blind price comparison |
| **Tawfeer** | Yes | Yes | Separate brand field enables direct matching |
| **Speed** | No (in product name) | No | Brand detected from product text; blind price comparison |
| **Talabia** | No (in product name) | Yes | Brand detected from product text |

---

## Dependencies

| Dependency | Role |
|------------|------|
| `setup_environment_2` | Snowflake credentials and environment initialization |
| `rapidfuzz` | Fuzzy string matching (token_sort_ratio, token_set_ratio, partial_ratio) |
| `wn` (Arabic WordNet) | Synonym expansion for category vocabulary (`omw-arb:1.4`) |
| `snowflake-connector-python` | Database queries |
| `openpyxl` | Excel export |
| SQL files in `Mapping/` | `current_sku_data.sql`, `raw_scraped_data_latest.sql`, `existing_mapping_query.sql` |
