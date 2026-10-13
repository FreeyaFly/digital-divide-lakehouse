-- WDI values for economies only; aggregates are rebuilt from countries in Gold.
select
    country_iso3 as country_code,
    indicator_code,
    indicator_name,
    year,
    value,
    unit
from {{ source('silver', 'silver_worldbank_indicator_values') }}
where not is_aggregate
