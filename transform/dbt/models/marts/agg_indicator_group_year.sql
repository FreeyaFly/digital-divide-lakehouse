-- WDI indicators aggregated from country rows to the world, regions and income groups, instead
-- of using WDI's own aggregates (which are revised with every release, ADR 0002).
-- Income groups come in two versions:
--   income_current  today's classification applied to every year ("today's low-income countries")
--   income_as_was   the classification in force on 31 December of that year (SCD2), so a
--                   country counts in the group it belonged to at the time
-- population_coverage is the share of the group's population with a value. Below 90% the
-- aggregate is unreliable: in 2025 only 10 countries report internet use (37% of the world
-- population) and the world value is 7 points above WDI's own estimate.
with population as (
    select country_code, year, value as population
    from {{ ref('fct_indicator_country_year') }}
    where indicator_code = 'SP.POP.TOTL'
),

countries as (
    select * from {{ ref('dim_country') }} where in_wdi
),

income_as_was as (
    select p.country_code, p.year, h.income_code, h.income_name
    from population as p
    inner join {{ ref('dim_country_income_history') }} as h
        on p.country_code = h.country_code
        and make_date(p.year, 12, 31) between h.valid_from and coalesce(h.valid_to, date'9999-12-31')
),

-- Each country-year belongs to one world, one region and two income groups.
members as (
    select
        p.country_code,
        p.year,
        p.population,
        inline(array(
            named_struct('group_type', 'world', 'group_code', 'WLD', 'group_name', 'World'),
            named_struct('group_type', 'region', 'group_code', c.region_code,
                         'group_name', c.region_name),
            named_struct('group_type', 'income_current', 'group_code', c.income_code_current,
                         'group_name', c.income_name_current),
            named_struct('group_type', 'income_as_was', 'group_code', a.income_code,
                         'group_name', a.income_name)
        ))
    from population as p
    inner join countries as c
        on p.country_code = c.country_code
    left join income_as_was as a
        on p.country_code = a.country_code and p.year = a.year
),

group_population as (
    select group_type, group_code, year, sum(population) as group_population, count(*) as group_countries
    from members
    where group_code is not null
    group by all
),

member_values as (
    select m.*, f.indicator_code, f.value, i.group_aggregation
    from members as m
    inner join {{ ref('fct_indicator_country_year') }} as f
        on m.country_code = f.country_code and m.year = f.year
    inner join {{ ref('dim_indicator') }} as i
        on f.indicator_code = i.indicator_code
    where m.group_code is not null
        and i.group_aggregation <> 'not_aggregated'
),

aggregated as (
    select
        group_type,
        group_code,
        any_value(group_name) as group_name,
        indicator_code,
        year,
        case any_value(group_aggregation)
            when 'sum' then sum(value)
            else sum(value * population) / sum(population)
        end as value,
        count(*) as countries_reporting,
        sum(population) as population_reporting
    from member_values
    group by group_type, group_code, indicator_code, year
)

select
    a.group_type,
    a.group_code,
    a.group_name,
    a.indicator_code,
    a.year,
    a.value,
    a.countries_reporting,
    g.group_countries,
    round(a.population_reporting / g.group_population, 4) as population_coverage,
    a.population_reporting / g.group_population >= {{ var('min_population_coverage', 0.9) }}
        as is_reliable
from aggregated as a
inner join group_population as g
    on a.group_type = g.group_type and a.group_code = g.group_code and a.year = g.year
