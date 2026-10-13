-- The "world" group in agg_speed_group_quarter contains every country row, so its tests per
-- quarter and connection type equal the country fact's totals.
with world as (
    select conn_type, quarter_start, tests
    from {{ ref('agg_speed_group_quarter') }}
    where group_type = 'world'
),

countries as (
    select conn_type, quarter_start, sum(tests) as tests
    from {{ ref('fct_speed_country_quarter') }}
    group by all
)

select c.conn_type, c.quarter_start, c.tests as country_tests, w.tests as world_tests
from countries as c
left join world as w
    on c.conn_type = w.conn_type and c.quarter_start = w.quarter_start
where w.tests is null or w.tests <> c.tests
