# Push Prices Handler

## Purpose

Low-level MaxAB API helper for uploading prices to the customized-price endpoint per cohort. Manages API authentication, builds upload Excel files in MaxAB's expected import format, chunks large pushes into 4000-row batches, and mirrors prices from the 9 custom cohorts to the 6 main/general cohorts (695, 61, 699, 697, 698, 696) automatically.

Lives at `Mustafa/modules/push_prices_handler.ipynb`. Loaded via `%run push_prices_handler.ipynb` by every module that needs to push prices.

---

## Public surface

| Function | Description |
|---|---|
| `push_prices(df, source_module, mode='testing')` | High-level entry: takes a (cohort_id, product_id, packing_unit_id, basic_unit_count, new_price) DataFrame, builds per-cohort Excels with auto-mirror to main cohorts, calls `post_prices()` per chunk, returns a per-cohort summary. |
| `post_prices(file_path, cohort_id)` | Low-level: POSTs a single Excel file to the MaxAB customized-price endpoint for a given cohort. Used directly by `non_food_cohorts_push.custom_push()`. |
| `get_access_token()` | Refreshes and caches the API access token from the configured credentials. **Does NOT print the response (token leak prevention).** |
| `_build_price_excel(group, file_path)` | Writes the per-cohort Excel template (sheet name `Worksheet`, columns: Product ID, Packing Unit ID, Basic Unit Count, Cohort, Price, Visibility, Execute At, Tags). |

---

## Cohort mirroring

Custom cohorts (700-1126) are pushed directly. After the custom push succeeds, the prices auto-mirror to the corresponding main/general cohorts:

| Custom cohort | Mirrors to |
|---|---|
| 700 (Cairo) | 695 |
| 701 (Giza) | 61 |
| 702 (Alexandria) | 699 |
| 703 (Delta West) | 697 |
| 704 (Delta East) | 698 |
| 1123-1126 (Upper Egypt) | 696 |

Mirroring is opt-in per call. Default behavior is to mirror.

---

## Chunking

| Cohort | Chunk size |
|---|---|
| 61 | 2000 rows |
| All others | 4000 rows |

Cohort 61 historically choked on 4000-row chunks; the 2000-row override has stuck. Chunk size lives in the per-call configuration; if you add new cohorts that misbehave, add an override there.

---

## Excel template

Each upload Excel has a single sheet named `Worksheet` with these columns:

| Column | Source |
|---|---|
| Product ID | `df['product_id']` |
| Packing Unit ID | `df['packing_unit_id']` |
| Basic Unit Count | `df['basic_unit_count']` |
| Cohort | The target cohort_id |
| Price | `df['new_price']` (rounded to 0.25 EGP) |
| Visibility (YES/NO) | "YES" by default; non-food handler overrides per row |
| Execute At | Cairo timestamp (now + 10 min by default) |
| Tags | Source module identifier (M2/M3/M4/M5) |

The `Worksheet` sheet name matches MaxAB's expected import format. Don't rename it.

---

## Mode

| Mode | Behavior |
|---|---|
| `testing` | Builds the Excel files locally but does NOT upload. Files saved to a temp directory. Use to inspect the upload payload before going live. |
| `live` | Uploads each chunk via `post_prices()`. |

---

## Error handling

- Each chunk's upload is wrapped in try/except. A chunk failure increments the failed count for that cohort but does NOT stop the rest.
- API errors are logged with the response body (excluding the access token, which is filtered out).
- Token refresh on 401 is automatic.

---

## Token leak prevention

The earlier version had `display(response.json())` inside `get_access_token()`, which leaked the access token into Slack alerts on failure. That display call has been removed. The current version logs only success/failure of token refresh.

---

## Dependencies

| Direction | Module |
|---|---|
| **Called by** | `module_2_initial_price_push`, `module_3_periodic_actions`, `module_4_hourly_updates`, `module_5_new_intros_invisible`, `manual_price_push`, `non_food_cohorts_push` (low-level path) |
| **Requires** | `setup_environment_2` (API credentials), `requests`, `pandas`, `xlsxwriter` |
| **External** | MaxAB API (customized-price endpoint), local filesystem (temp xlsx files) |
