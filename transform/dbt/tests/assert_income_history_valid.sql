-- SCD2 rules for dim_country_income_history:
--   1. a country's versions never overlap in time
--   2. every WDI economy has exactly one current version
--   3. the current version equals the income group the Indicators API reports today
--      (checked by hand on 2026-10-12: 217 of 217 agree)
with versions as (
    select *, coalesce(valid_to, date'9999-12-31') as valid_to_or_open
    from {{ ref('dim_country_income_history') }}
),

overlaps as (
    select a.country_code, 'overlapping versions' as problem
    from versions as a
    inner join versions as b
        on a.country_code = b.country_code
        and a.income_version_key < b.income_version_key
        and a.valid_from <= b.valid_to_or_open
        and b.valid_from <= a.valid_to_or_open
),

current_versions as (
    select country_code, count(*) as n, any_value(income_code) as income_code
    from versions
    where is_current
    group by country_code
),

current_problems as (
    select
        c.country_code,
        case
            when v.country_code is null then 'no current version'
            when v.n > 1 then 'several current versions'
            else 'current version differs from WDI'
        end as problem
    from {{ ref('dim_country') }} as c
    left join current_versions as v
        on c.country_code = v.country_code
    where c.in_wdi
        and (v.country_code is null or v.n > 1 or v.income_code <> c.income_code_current)
)

select * from overlaps
union all
select * from current_problems
