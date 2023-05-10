select
    {{ dbt_utils.star(source('emplois', 'organisations')) }}
from {{ source('emplois', 'organisations') }}
where type in ('CHRS', 'CHU', 'RS_FJT', 'OIL')
