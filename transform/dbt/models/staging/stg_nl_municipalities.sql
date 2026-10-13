select
    gemeente_code as municipality_code,
    gemeente_naam as municipality_name,
    provincie_code as province_code,
    provincie_naam as province_name,
    boundaries_retrieved
from {{ source('silver', 'silver_nl_municipalities') }}
