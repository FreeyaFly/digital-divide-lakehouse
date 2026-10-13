-- SCD2 history of World Bank income groups, FY89 to the current fiscal year.
-- Consecutive fiscal years with the same group collapse into one version (gaps and islands): a
-- new version starts when the group changes or after years without a classification (e.g.
-- Venezuela, unclassified FY22–FY26). A fiscal year runs from July 1 to June 30, so FY27 is
-- valid from 2026-07-01. The version for the latest fiscal year is open-ended (is_current).
-- Former economies (USSR, Yugoslavia, ...) are left out: they have no row in dim_country.
with classifications as (
    select h.country_code, h.fiscal_year, h.income_code
    from {{ ref('wb_income_history') }} as h
    inner join {{ ref('dim_country') }} as c
        on h.country_code = c.country_code
),

current_fiscal_year as (
    select max(fiscal_year) as fiscal_year from {{ ref('wb_income_history') }}
),

islands as (
    select
        *,
        -- Equal for consecutive years of one unbroken spell in the same group.
        fiscal_year - row_number() over (
            partition by country_code, income_code order by fiscal_year
        ) as island
    from classifications
),

versions as (
    select
        country_code,
        income_code,
        min(fiscal_year) as fiscal_year_from,
        max(fiscal_year) as fiscal_year_to
    from islands
    group by country_code, income_code, island
)

select
    concat(v.country_code, '|', v.fiscal_year_from) as income_version_key,
    v.country_code,
    v.income_code,
    {{ income_name('v.income_code') }} as income_name,
    v.fiscal_year_from,
    v.fiscal_year_to,
    make_date(v.fiscal_year_from - 1, 7, 1) as valid_from,
    case
        when v.fiscal_year_to = cfy.fiscal_year then null
        else make_date(v.fiscal_year_to, 6, 30)
    end as valid_to,
    v.fiscal_year_to = cfy.fiscal_year as is_current
from versions as v
cross join current_fiscal_year as cfy
