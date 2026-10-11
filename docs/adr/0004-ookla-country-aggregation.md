# ADR 0004: Global country-level aggregation of Ookla tiles

- **Status:** Accepted (validated 2026-10-12)
- **Date:** 2026-10-12

## Context
Week 3 mapped Dutch tiles to municipalities by filling each municipality with H3 cells at resolution 9 and joining on the cell id (ADR 0003). That does not scale to the world: covering all land at resolution 9 takes about 1.5 billion cells.

Week 4 assigns every tile in Bronze (80 M rows, 8 quarters × fixed/mobile) to a country and aggregates per country and quarter, so that Gold (week 5) can join speed tests to World Bank indicators.

## Decision

### Boundaries: Natural Earth 1:10m Admin 0, v5.1.2 (Notebook 34)
| Candidate | Size | Licence | Verdict |
|---|---|---|---|
| **Natural Earth 1:10m Admin 0** | 13 MB GeoJSON | Public domain | ✅ chosen |
| geoBoundaries CGAZ ADM0 | 401 MB | CC BY 4.0 | Too large for the gain |
| GADM | — | Non-commercial | Licence risk |

- The file is pinned to the `v5.1.2` tag and its **SHA-256 is checked** before use; the raw GeoJSON is archived in the Volume.
- Boundaries follow Natural Earth's de facto lines. This is a data-source choice, not a position on disputed territory.

### Country codes (`notebooks/country_codes.py`, unit-tested)
Natural Earth's codes need corrections before they match WDI:
- `ISO_A3` is `-99` for 22 units, including **France and Norway**; `ISO_A3_EH` fills most of them.
- `WB_A3` holds **old** World Bank codes (`ROM`, `ZAR`, `TMP`) and is not used.
- Overrides for WDI: Kosovo → `XKX`, Jersey and Guernsey → `CHI`, Åland → `FIN`.
- Units with no ISO code at all (Somaliland, N. Cyprus, Siachen, …) **keep Natural Earth's own `ADM0_A3`** and are not merged into another country; Gold decides how to treat them.
- Antarctica is excluded: it spans every meridian and contains the pole, and has no WDI data.

The codes are reconciled with the 217 WDI countries **in both directions**: every WDI country has a boundary, and the 32 boundary codes without a WDI country must equal an expected list (territories such as Taiwan, Western Sahara and the Falklands, plus the de facto units). Any difference stops the run.

### Spatial join: H3 tessellation into core cells and border chips
Each country polygon is cut along **H3 resolution-5 cells** (~250 km²) with `h3_tessellateaswkb`:
- a **core** cell lies entirely inside one country, so a tile in it is assigned by cell id alone;
- a **border** cell carries the "chip" where it overlaps the country, and a tile in it is tested with `st_contains` against that small chip only.

Notebook 35 assigns tiles in two passes:
1. **Own cell:** core → by id; border → `st_contains` against the cell's chip(s).
2. **Neighbours:** a tile that pass 1 leaves unplaced is tested against the chips of the six neighbouring cells. H3 decides a point's cell on the sphere while chips are planar, so a point within metres of a cell edge can lie in the neighbour's chip (0.02% of points in a probe).

Tiles still unplaced are outside every country (sea, or a coastline drawn inland at 1:10m). The result is exact for the planar polygons, unlike week 3's centre-of-cell approximation, while the geometry work stays at borders and coasts.

### Tables
| Table | Grain | Rows |
|---|---|---|
| `silver_country_boundaries` | Natural Earth unit | 258 |
| `silver_country_h3_chips` | country part × H3 cell | 568,683 (78.7 MB) |
| `silver_ookla_tile_country` | distinct tile (quadkey) | 18,305,611 |
| `silver_ookla_country_quarter` | connection type × quarter × country | 3,757 |

- **No tile-level global Silver.** Copying 80 M Bronze rows to add one country code would double storage. Instead, a tile's country is stored once in the mapping, and the aggregate is built from Bronze + mapping.
- **The mapping is incremental:** only tiles not mapped before are assigned. New quarters, or extending to all 31 quarters, only pay for new tiles. A change of boundary version or H3 resolution re-maps everything.
- **The aggregate stores additive measures** (Σ tests, Σ devices, Σ speed × tests, Σ latency × tests), never averages. An average of averages is wrong: two countries with 1,000,000 tests at 100 Mbps and 1,000 tests at 10 Mbps average to 99.9 Mbps, not 55. With sums, Gold can re-weight correctly at any level (region, income group).
- This moves the country-level aggregation from dbt (planned in ADR 0003) into PySpark, because aggregating 80 M rows is the Spark workload. dbt Gold keeps the joins with WDI and the regional roll-ups.

### Serverless constraints
- Serverless compute **rejects `DataFrame.cache()`** with an exception. Intermediate results read several times are written to staging tables (`stage_ookla_*`) and dropped at the end.
- The working directory is not guaranteed, so Notebook 34 puts its own workspace folder on `sys.path` before importing `country_codes.py`.

## Results (2026-10-12)

### Checks (all with an external expected value; any failure stops the run)
| Check | Expected value from | Result |
|---|---|---|
| Source file unchanged | SHA-256 of the v5.1.2 tag | ✅ |
| Invalid geometry repaired | shapely: only Egypt (ring self-intersection), area unchanged | ✅ 90.730189 → 90.730189 deg² |
| Total boundary area (excl. Antarctica) | shapely: 15,378.276 deg² | ✅ −0.0000% |
| Codes vs WDI | 217 countries; 32 expected codes not in WDI | ✅ exact match |
| Chips tile the boundaries | Σ chip area = Σ boundary area | ✅ −0.0000% |
| Core cells unique | — | ✅ 487,209 = 487,209 |
| Tile counts | `ingestion_versions`, per file | ✅ all 16 files |
| Tests per country, 2026 Q3 | independent shapely (GEOS) assignment of the same files, 18 countries × fixed/mobile + unassigned | ✅ **38 of 38 identical** |
| Unassigned share of tests | locally 1.99% (fixed) / 2.50% (mobile); fails above 4% | ✅ 1.79–2.50% per file |
| The Netherlands vs week 3 | `silver_ookla_nl_tiles` (PDOK boundaries, H3 res 9); fails above 2% | ✅ fixed −0.75% to −1.00%, mobile −1.2% to −2.0% |
| Idempotency | second run | ✅ 0 new tiles, MERGE 0 / 0 / 0, identical outputs |

### Mapping
| Method | Tiles | Share |
|---|---|---|
| Core cell (id only) | 15,177,016 | 82.9% |
| Border chip (one `st_contains`) | 2,692,653 | 14.7% |
| Neighbouring chip (pass 2) | 254 | <0.01% |
| Outside every country | 435,688 | 2.4% |

18.3 M distinct tiles, about 80% more than estimated: fixed and mobile tiles overlap only partly.

## Performance
All times on Databricks Free Edition serverless.

### Tessellation: the single-partition trap
| Version | Tasks | Time |
|---|---|---|
| As first written | 1 | >9 min, interrupted |
| With `.repartition(32)` before tessellating | 34 | 279 s (re-run: 264 s) |

The boundary table is 258 rows in one 13 MB file, so Spark read it as **one partition**: partitions follow input bytes, not compute cost. `posexplode` turned 258 countries into ~4,100 polygon parts inside that same partition, and every part was tessellated by one task. Repartitioning the parts before the expensive expression spreads them over all cores. The remaining time is bounded by the largest single part (Russia's mainland: Russia alone has 67,852 chips), i.e. skew. It runs only when the boundaries change, so it is not optimised further.

### Choice of resolution (five contrasting countries: NLD, NOR, IDN, BRA, CHE)
| Resolution | Chips | Border chips | Avg. border chip | Tessellation |
|---|---|---|---|---|
| 4 | 7,481 | 34.4% | 409 B | 8.7 s |
| **5** | 44,761 | **16.1%** | 222 B | 42.5 s |
| 6 | 296,315 | 7.2% | 150 B | 257.9 s |

Each finer level multiplies chips and tessellation time by ~6 and roughly halves the share of border cells. Resolution 5 keeps the one-off tessellation at a few minutes while ~83% of tiles need no geometry test, and border chips are small (~13 vertices).

### Notebook 35
| Step | First run (18.3 M new tiles) | Incremental run (0 new) |
|---|---|---|
| Distinct new tiles | 20.1 s | 10.4 s |
| Pass 1 (own cell) | 7.6 s | 2.6 s |
| Pass 2 (neighbours) + MERGE | 30.6 s | 3.1 s |
| Aggregate 80 M rows → 3,757 | 16.1 s | 14.2 s |

**18.3 M tiles assigned to countries in under a minute.**

On a 0.1% sample (6,569 tiles of fixed 2026 Q3), the chip method and a plain bounding-box + `st_contains` join both took about 2 s and agreed on all 6,456 assigned tiles. At this size fixed overhead dominates, so **the sample does not show which method is faster**; the evidence for the chip method is the full run above, where 83% of tiles need no geometry test at all.

## Issues found
- **`cache()` fails on serverless.** Found in the documentation before the first run; replaced with staging tables.
- **One task for the whole world** (see Performance). Not visible in local tests; found on the first Databricks run.
- **`st_buffer(geom, 0)` can drop part of a polygon.** In a probe it removed one lobe of a bow-tie polygon. For Egypt it repairs the ring without changing the area, and Notebook 34 fails if a repair changes the area by more than 0.01%.
- **The antimeridian is handled.** A probe confirmed that polygons touching ±180° (Fiji, Russia's far east) tessellate into narrow chips whose areas add up exactly.

## Consequences
- **About 2% of tests fall outside every country**, almost all on coasts where the 1:10m coastline lies inland of the tile centre. A future option is to assign them to the nearest country within a small distance.
- **The global method finds ~1% fewer Dutch tests than week 3 (mobile up to 2%), in every quarter.** PDOK municipal boundaries include coastal water; Natural Earth's coastline is generalised. The difference is systematic, not random. The mobile margin is thin against the 2% tolerance; a separate mobile tolerance is planned when the notebook next changes (week 6).
- Natural Earth's Netherlands includes the Caribbean Netherlands, so the comparison with week 3 is limited to the European bounding box. Gold must decide whether `NLD` includes them.
- **Ookla publishes almost no Russian tiles** (about 120 fixed and 150 mobile tiles in 2026 Q3); Russia's figures are not representative.
- Units without a WDI country (Taiwan, Western Sahara, de facto units, small territories) are kept in Silver; Gold decides whether to show them.
- Extending to 31 quarters (deferred until after week 7) only needs Notebook 31 with `latest_quarters=31` and a re-run of 33 and 35; only new tiles are mapped.
- Attribution: boundaries from Natural Earth (public domain).
