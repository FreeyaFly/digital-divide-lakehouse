-- Every quarter with Ookla data, for both the country and the municipality facts.
with quarters as (
    select quarter_start from {{ ref('stg_ookla_country_quarter') }}
    union
    select quarter_start from {{ ref('stg_ookla_nl_municipality_quarter') }}
)

select
    quarter_start,
    date_sub(add_months(quarter_start, 3), 1) as quarter_end,
    year(quarter_start) as year,
    quarter(quarter_start) as quarter,
    concat(year(quarter_start), ' Q', quarter(quarter_start)) as quarter_label
from quarters
