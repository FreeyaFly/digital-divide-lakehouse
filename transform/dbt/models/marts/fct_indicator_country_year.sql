-- One WDI value per economy, indicator and year (long format). Missing values have no row.
-- GDP per capita is kept in both current US$ and PPP; PPP is the default for comparisons.
select
    country_code,
    indicator_code,
    year,
    value
from {{ ref('stg_wb_indicator_values') }}
