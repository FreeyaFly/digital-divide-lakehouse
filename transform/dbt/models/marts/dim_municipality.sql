-- Current Dutch municipalities (PDOK); all quarters use these boundaries (ADR 0003).
select
    municipality_code,
    municipality_name,
    province_code,
    province_name,
    boundaries_retrieved
from {{ ref('stg_nl_municipalities') }}
