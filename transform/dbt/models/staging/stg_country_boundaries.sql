-- One row per country code. Several Natural Earth units can share a code (France and
-- Clipperton, Jersey and Guernsey as CHI); the name comes from the unit carrying the code
-- itself when there is one.
select
    country_code,
    coalesce(
        max(case when ne_adm0_a3 = country_code then name end),
        min(name)
    ) as boundary_name,
    count(*) as natural_earth_units
from {{ source('silver', 'silver_country_boundaries') }}
where not excluded
group by country_code
