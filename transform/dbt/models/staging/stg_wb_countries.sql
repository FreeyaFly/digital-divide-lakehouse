-- WDI economies only; regional and income-group aggregates are rebuilt in Gold from countries.
select
    country_iso3 as country_code,
    iso2,
    country_name,
    region_code,
    region_name,
    income_code,
    income_name,
    lending_code,
    capital_city,
    longitude,
    latitude
from {{ source('silver', 'silver_worldbank_countries') }}
where not is_aggregate
