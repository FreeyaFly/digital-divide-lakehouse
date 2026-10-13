-- Gold keeps every Silver speed test: totals per connection type and quarter must match.
with silver as (
    select conn_type, period_start as quarter_start, sum(tests) as tests
    from {{ source('silver', 'silver_ookla_country_quarter') }}
    group by all
),

gold as (
    select conn_type, quarter_start, sum(tests) as tests
    from {{ ref('fct_speed_country_quarter') }}
    group by all
)

select
    coalesce(s.conn_type, g.conn_type) as conn_type,
    coalesce(s.quarter_start, g.quarter_start) as quarter_start,
    s.tests as silver_tests,
    g.tests as gold_tests
from silver as s
full outer join gold as g
    on s.conn_type = g.conn_type and s.quarter_start = g.quarter_start
where s.tests is null or g.tests is null or s.tests <> g.tests
