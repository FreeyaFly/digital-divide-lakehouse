# ADR 0005: dbt Gold layer

- **Status:** Accepted (validated 2026-10-12)
- **Date:** 2026-10-12

## Context
Silver holds clean but source-shaped tables: WDI values in long format, Ookla tests per country or Dutch tile (ADR 0002–0004). Week 5 builds the analytical layer that Power BI reads in week 7: a star schema, regional and income-group aggregates, and the history of World Bank income classifications as a slowly changing dimension (SCD2).

## Decision

### Tooling and connection
- **dbt Core 1.12 with dbt-databricks**, run against the Free Edition **SQL warehouse** (serverless SQL, no cluster to manage). The project lives in `transform/dbt/`.
- `profiles.yml` is committed but contains only `env_var()` calls. Host, HTTP path and token come from user-level environment variables.
- The token is a personal access token with the **`sql` scope only** (BI Tools), 30-day lifetime, auto-scoping off. It is never written to a file or typed into a command: it is read with `Read-Host` in a standalone PowerShell window.
- Schemas follow dbt's default naming: `digital_divide_dev_staging` (views, seed), `digital_divide_dev_gold` (tables Power BI reads), `digital_divide_dev_snapshots`.

### Model layers
| Layer | Models | Materialisation |
|---|---|---|
| Staging | `stg_wb_countries`, `stg_wb_indicator_values`, `stg_country_boundaries`, `stg_ookla_country_quarter`, `stg_ookla_nl_municipality_quarter`, `stg_nl_municipalities` | views |
| Dimensions | `dim_country`, `dim_country_income_history` (SCD2), `dim_indicator`, `dim_quarter`, `dim_municipality` | tables |
| Facts | `fct_indicator_country_year`, `fct_speed_country_quarter`, `fct_speed_municipality_quarter` | tables |
| Aggregates and marts | `agg_indicator_group_year`, `agg_speed_group_quarter`, `mart_country_year` | tables |
| Snapshot | `snap_wb_country_income` | dbt snapshot |

![dbt lineage](../images/week5-dbt-lineage.png)

### Income classification history (SCD2)
The Indicators API only returns today's income group, and a dbt snapshot only records changes from its first run (the next reclassification is July 2027). The World Bank publishes the full history as `OGHIST.xlsx`, a resource of the WDI dataset (CC BY 4.0):
- `python -m ingestion.worldbank.income_history` downloads it, **checks a pinned SHA-256**, and writes a long CSV seed (`wb_income_history`: 8,095 rows, 224 economies, FY89–FY27). A new release stops the script until the change is reviewed.
- `dim_country_income_history` collapses consecutive fiscal years with the same group into one version with a **gaps-and-islands** query (`fiscal_year − row_number()` is constant within a run). A new version starts on a change of group or after unclassified years. FY27 is valid from 2026-07-01; the latest version is open-ended (`valid_to` null, `is_current`).
- `snap_wb_country_income` (check strategy on income group, region and lending category) records future changes as the API reports them.
- Checked before building: the FY27 column of the workbook equals today's API income group for **217 of 217** economies.

Examples: Venezuela is unclassified in FY22–FY26 and returns as lower-middle income in FY27 (two versions around a gap); Russia has six versions and is high income since FY25; Costa Rica is high income since FY26.

### Modelling decisions
1. **Units outside WDI stay in.** `dim_country` has 249 rows: 217 WDI economies and 32 units with an Ookla boundary but no WDI data (Taiwan, Western Sahara, small territories, de facto units), flagged `in_wdi = false`. They carry 1.5% (fixed) to 1.8% (mobile) of all tests, mostly Taiwan. They appear in speed facts and in the world speed total, but not in WDI joins or in region and income-group aggregates. Gold totals therefore equal Silver exactly.
2. **The Caribbean Netherlands stays in `NLD`.** Natural Earth includes it in the Netherlands; it holds 0.2% of Dutch fixed tests and 0.14% of mobile tests.
3. **Low samples are flagged by a generic rule, not by country.** `is_low_sample` marks a country-quarter row with fewer than 1,000 tests (variable `min_country_tests`). In fixed 2026 Q3, 33 units are flagged; besides small islands they include Russia, Belarus, Iran, North Korea and Eritrea, so sparse data is itself a finding.
4. **Municipal mobile data is pooled over four quarters.** In 2026 Q3, 161 of 342 municipalities have fewer than 100 mobile tests (`is_low_sample`, variable `min_municipality_tests`). Each row also carries the additive sums of the last four quarters (`*_4q`); pooled, only 12 municipalities stay below 100 tests. Because the measures are sums, pooling is plain addition, never an average of averages. `assert_municipality_speed_complete` guarantees no quarter is missing, which the four-row window relies on.
5. **Group aggregates are rebuilt from countries**, not taken from WDI, because WDI revises its aggregates in every release (ADR 0002). `dim_indicator.group_aggregation` sets the rule per indicator:
   - `population_weighted` (shares, per-100 and per-capita values): Σ value × population / Σ population;
   - `sum` (population);
   - `not_aggregated` for five indicators whose correct weight is not in the data (female/male internet use, rural/urban electricity access, gross tertiary enrolment). No value is better than a wrongly weighted one.
6. **Two income-group views.** `income_current` applies today's classification to every year; `income_as_was` uses the classification in force on 31 December of the year (indicators) or on the first day of the quarter (speed). They answer different questions: internet use in low-income countries in 2005 was **2.6%** for the 56 countries classified low income at the time, but **0.9%** for today's 25 low-income countries.
7. **Coverage travels with every aggregate.** `population_coverage` is the share of the group's population that reports a value; `is_reliable` requires 90% (variable `min_population_coverage`). Measured against WDI's own world estimate of internet use: within −1.1 to +2.1 points in 2005–2024 (coverage 94–100%), but **+7.5 points in 2025**, when only 10 countries (37% of the world population) report.
8. **Speed for world and groups is test-weighted** from the additive country sums. `world` includes every unit with tests; regions and income groups only WDI economies.
9. **`mart_country_year`** puts indicators and yearly speed side by side for cross-country charts. GDP per capita is kept in PPP (default) and current US$. Yearly speed pools the quarters of a calendar year; `quarters_fixed` / `quarters_mobile` say how many (2025 is the only full year in the 2024 Q4 – 2026 Q3 range).

### Tests (54)
Generic tests on keys, not-null, accepted values and relationships, a reusable `unique_combination` test for composite keys, and six singular tests with external expected values:

| Test | Expected value from |
|---|---|
| `assert_speed_tests_conserved` | Silver test totals per quarter and connection type |
| `assert_world_speed_conserved` | the country fact's totals |
| `assert_dim_country_coverage` | 217 WDI economies, all with a boundary |
| `assert_income_history_valid` | no overlapping versions; one current version per economy; current version = today's API income group |
| `assert_world_internet_matches_wdi` | WDI's own world estimate, within 2.5 points where coverage ≥ 90% |
| `assert_municipality_speed_complete` | 342 municipalities × 16 quarter-type combinations; test totals equal the Silver tiles |

## Results (2026-10-12)
| Run | Result |
|---|---|
| Minimal slice (`dim_country`, `fct_speed_country_quarter`, seed) | PASS=24, 39.6 s |
| Full build: 1 seed, 6 views, 11 tables, 1 snapshot, 54 tests | **PASS=73**, 96.2 s |
| Second full build (idempotency) | PASS=73, 64.1 s; snapshot unchanged (217 rows, all current) |

| Model | Rows |
|---|---|
| `dim_country` | 249 (217 in WDI) |
| `dim_country_income_history` | 560 versions |
| `fct_indicator_country_year` | 60,897 |
| `fct_speed_country_quarter` | 3,757 |
| `fct_speed_municipality_quarter` | 5,472 (342 × 16) |
| `agg_indicator_group_year` | 3,512 |
| `agg_speed_group_quarter` | 256 |
| `mart_country_year` | 5,858 |

Fixed download speed, 2026 Q3, test-weighted: high income **329.9** Mbps, upper-middle 199.5, lower-middle 77.9, low income **44.5** (a 7.4× gap); world 242.5. The Netherlands: 324.9 Mbps, identical to the independent shapely check of ADR 0004.

## How the models were checked before running
Before asking for a run, every model and test was executed read-only: a scratch copy of the project with all models set to `ephemeral` was compiled (`dbt compile --no-populate-cache`, no connection needed), so each model and test became one self-contained `SELECT`, which was run on the SQL warehouse through the Statement Execution API. All 17 models executed and every test returned zero failing rows before the first full build.

## Consequences
- Reports must state which income-group view they use; the two can tell different stories.
- Aggregates for recent years are often unreliable (coverage below 90%); charts should filter on `is_reliable`.
- The OGHIST seed must be regenerated each July (new fiscal year): the pinned SHA-256 makes the change explicit.
- The dbt token expires after 30 days and must be regenerated; the Databricks CLI login (used for imports, exports and read-only checks) is separate and also expires.
- Not yet orchestrated: dbt runs by hand. Week 6 adds Airflow (Cosmos) and CI (`dbt build` on a schedule, `dbt parse` in GitHub Actions).
