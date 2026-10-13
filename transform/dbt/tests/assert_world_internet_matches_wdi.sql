-- Our population-weighted world share of internet users must stay close to WDI's own world
-- estimate (WLD) in every year where the aggregate is reliable (population coverage >= 90%).
-- WDI imputes missing countries, so the two differ slightly: measured on 2026-10-12, -1.1 to
-- +2.1 percentage points for 2005–2024. Tolerance: 2.5 points.
with ours as (
    select year, value
    from {{ ref('agg_indicator_group_year') }}
    where group_type = 'world' and indicator_code = 'IT.NET.USER.ZS' and is_reliable
),

wdi as (
    select year, value
    from {{ source('silver', 'silver_worldbank_indicator_values') }}
    where country_iso3 = 'WLD' and indicator_code = 'IT.NET.USER.ZS'
)

select o.year, o.value as ours, w.value as wdi, o.value - w.value as diff_points
from ours as o
inner join wdi as w
    on o.year = w.year
where abs(o.value - w.value) > {{ var('world_internet_tolerance_points', 2.5) }}
