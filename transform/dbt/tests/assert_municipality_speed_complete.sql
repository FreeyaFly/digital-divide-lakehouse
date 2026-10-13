-- Every municipality has a row for every quarter and connection type (week 3: all 342 have
-- data in all 16 combinations), and the tests add up to the Silver tiles. The 4-quarter
-- window in fct_speed_municipality_quarter relies on there being no missing quarters.
with fact as (
    select * from {{ ref('fct_speed_municipality_quarter') }}
),

expected as (
    select
        (select count(*) from {{ ref('dim_municipality') }})
        * (select count(distinct conn_type, quarter_start) from fact) as rows_expected
),

silver as (
    select sum(tests) as tests from {{ source('silver', 'silver_ookla_nl_tiles') }}
)

select 'row count' as check_name, (select count(*) from fact) as actual, e.rows_expected as expected
from expected as e
where (select count(*) from fact) <> e.rows_expected

union all

select 'tests', (select sum(tests) from fact), s.tests
from silver as s
where (select sum(tests) from fact) <> s.tests
