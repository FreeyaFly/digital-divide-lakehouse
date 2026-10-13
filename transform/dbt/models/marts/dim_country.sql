-- Every country code that has WDI data or an Ookla boundary. Units outside WDI (Taiwan,
-- Western Sahara, small territories, de facto units) are kept with in_wdi = false: they have
-- speed tests but no region, income group or indicators (ADR 0005).
with wdi as (
    select * from {{ ref('stg_wb_countries') }}
),

boundaries as (
    select * from {{ ref('stg_country_boundaries') }}
)

select
    coalesce(w.country_code, b.country_code) as country_code,
    coalesce(w.country_name, b.boundary_name) as country_name,
    w.iso2,
    w.region_code,
    w.region_name,
    w.income_code as income_code_current,
    w.income_name as income_name_current,
    w.country_code is not null as in_wdi,
    b.country_code is not null as has_boundary,
    w.longitude,
    w.latitude
from wdi as w
full outer join boundaries as b
    on w.country_code = b.country_code
