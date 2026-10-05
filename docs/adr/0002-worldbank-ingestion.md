# ADR 0002: World Bank ingestion

- **Status:** Accepted (validated 2026-10-05)
- **Date:** 2026-10-05

## Context
The first source is the World Bank Indicators API v2, World Development Indicators (WDI, source 2). It is small (about 6,900 rows per indicator) and refreshed a few times a year. Its quirks drive several decisions below:

- Errors come back as **HTTP 200** with a body like `[{"message": [...]}]`.
- Paging metadata mixes types: `per_page` is sometimes a string.
- Responses are `[page_metadata, records]`, which Spark cannot read as one schema.
- The response is the same entity list for every indicator: 217 countries plus regional and income aggregates.
- **Income-group aggregates (High income, Low income, ...) come with an empty `countryiso3code`.** Only their two-letter `country.id` is set (e.g. `XD`), which maps to the ISO3 code (`HIC`) via the country list.

Architecture A from [ADR 0001](0001-platform-choice.md) applies: ADLS is the raw archive, Databricks Free Edition reads from a UC Volume, and Spark never reads ADLS directly.

## Decision

### Indicators (14, in `ingestion/config/worldbank.toml`)

| Group | Indicators |
|---|---|
| Digital access | `IT.NET.USER.ZS`, `IT.NET.USER.FE.ZS`, `IT.NET.USER.MA.ZS`, `IT.NET.BBND.P2`, `IT.CEL.SETS.P2`, `IT.NET.SECR.P6` |
| Infrastructure | `EG.ELC.ACCS.ZS`, `EG.ELC.ACCS.RU.ZS`, `EG.ELC.ACCS.UR.ZS`, `SP.URB.TOTL.IN.ZS` |
| Socio-economic | `NY.GDP.PCAP.CD`, `NY.GDP.PCAP.PP.CD`, `SP.POP.TOTL`, `SE.TER.ENRR` |

- Female and male internet use enable a **gender gap**; urban and rural electricity access enable an **urban–rural gap**.
- GDP per capita is kept in both **current US$** and **PPP**. PPP is the default for cross-country comparison; a USD toggle is a reporting stretch goal.
- Fixed telephone lines were left out. Adding an indicator later is a one-line config change, and the next run fetches only that indicator.

### Landing (Python, `ingestion/worldbank`)
- **Format:** gzipped JSON Lines, one API record per line, **records unchanged**; only the paging envelope is dropped (it goes into the run manifest). gzip runs with a fixed header timestamp (`mtime=0`), so identical records always produce identical bytes.
- **Layout:** `landing/worldbank/source_updated=<WDI lastupdated>/indicators/indicator=<code>/page-NNNN.jsonl.gz`, plus `countries/`, `_manifests/` and `_state/watermark.json`.
- **Incremental, level 1 (source):** the watermark stores the WDI `lastupdated` date, the year range and the landed indicators.
  - If the release or the year range changed, everything is re-landed into a new partition.
  - Otherwise only indicators missing from the watermark are fetched.
  - If nothing is missing, the run is skipped with no data API calls.
- **Commit point:** data files first, then the manifest, then the watermark. A crash before the watermark simply redoes the run, and the fixed paths make that an overwrite.
- **Auth:** `DefaultAzureCredential` (the developer's `az login` session, or a service principal in a container). No account keys.
- **Client:** retries with exponential backoff on 429/5xx; it fails loudly on non-JSON responses, on error payloads and on unexpected shapes.

### Bridge ADLS → Volume (Notebook 10)
- The notebook lists and downloads over the ADLS REST API with a **user-delegation SAS** limited to the `landing` container, with read and list only and at most 7 days of validity. The SAS lives in the Databricks secret scope `ddlake`.
- Files already present in the Volume with the same size are skipped.

### Bronze (Notebook 10)
- **`COPY INTO`** loads `bronze_worldbank_observations` and `bronze_worldbank_countries`. COPY INTO tracks loaded files, so a re-run inserts nothing.
- **All fields are stored as strings** (`primitivesAsString`), so type inference can never break a load. Every row also gets `_source_file`, `_source_updated` and `_loaded_at`.
- Bronze keeps **every** source version, which gives a history of WDI revisions.
- Bronze rows are reconciled **per file** against the run manifests, and any mismatch fails the notebook.

### Silver (Notebook 20)
- Silver is built from the **latest** source version only.
- **Blank ISO3 codes are resolved via iso2 → ISO3 before de-duplication.**
- Types are converted with `try_cast`, because serverless runs with ANSI mode on and a plain `CAST('' AS DOUBLE)` raises an error.
- Null values are dropped: a missing value means there is no row.
- Aggregates are **kept and flagged** with `is_aggregate` (region `NA`); Gold decides how to use them.
- **Incremental, level 2 (record):** a `MERGE` keyed on `(country_iso3, indicator_code, year)` compares a SHA-256 `row_hash`. It updates only rows whose content changed, inserts new rows, and deletes rows the new release no longer contains. Deleting is correct here because every release is a full snapshot of the selected indicators.
- **Quality gates** (any failure stops the notebook):
  - no failed numeric casts;
  - de-duplication drops **zero** rows (within one release every key must be unique);
  - row conservation: Silver input = Bronze rows − null values;
  - unique keys, years within range, the same indicator count as Bronze, and no unknown entities.

## Validated on Free Edition (2026-10-05)
- Secret scopes work (`databricks secrets create-scope`).
- Python can write files into UC Volumes on serverless compute.
- `COPY INTO` works from Volumes, including `_metadata.file_path`.

## Results (WDI release 2026-07-13)

| Layer | Result |
|---|---|
| Landing | 15 files, 96,755 rows, **0.92 MB** gzipped (24.7 MB as plain JSONL, 26× smaller) |
| Bronze | 96,460 observation rows + 295 country rows, reconciled per file |
| Silver | 74,259 values (22,201 nulls dropped); 217 countries, 47 aggregates, 14 indicators |
| Idempotency | Second runs: landing skipped, 0 files copied, 0 rows inserted by COPY INTO, MERGE 0/0/0 |

## Issue found during validation
The first Silver run passed every check but **silently lost 1,456 rows**. De-duplication ran before the key was resolved, so the five income-group aggregates, which share an empty ISO3 code, collapsed into one "entity". The Silver checks could not see this, because they only inspect rows that reached Silver. It was caught because Bronze row counts did not add up (95,004 vs 96,460).

The fix resolves ISO3 via iso2 first, makes de-duplication fail if it drops anything, and adds a row-conservation gate. The repaired run inserted exactly the 1,098 missing income-group values, with 0 updates and 0 deletes. "Not classified" has no values at all, which is why there are 47 aggregates rather than 48.

## Coverage notes for analysis
- Female and male internet use: about 1,360 values across 137 countries; treat them as a **subsample**.
- Secure Internet servers start in **2010**.
- PPP GDP covers 203 countries, against 214 for current US$.
- The latest year differs by indicator (2024 or 2025), so "latest value" needs an explicit rule in Gold.
- Mobile subscriptions per 100 people can exceed 100, because they count SIMs rather than people.

## Consequences
- **The SAS must be regenerated at least weekly** (the current one expires 2026-10-11). Week 6 automates this in Airflow; until then it is a manual step and an expired SAS fails Notebook 10 with an explicit 403 message.
- Silver holds only the current country attributes. The history of income-group reclassification each July is handled by a dbt snapshot in week 5.
- The notebooks are run by hand, in the order ingestion → 10 → 20, until week 6 orchestration.
