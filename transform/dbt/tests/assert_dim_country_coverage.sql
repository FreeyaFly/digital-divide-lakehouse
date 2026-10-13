-- All 217 WDI economies are in dim_country, and every WDI economy has a boundary (ADR 0004).
select 'wdi economies' as check_name, count_if(in_wdi) as actual, 217 as expected
from {{ ref('dim_country') }}
having count_if(in_wdi) <> 217

union all

select 'wdi economies without boundary', count_if(in_wdi and not has_boundary), 0
from {{ ref('dim_country') }}
having count_if(in_wdi and not has_boundary) <> 0
