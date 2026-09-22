select
    {{ pilo_star(ref('stg_contacts_commandeurs')) }}
from {{ ref('stg_contacts_commandeurs') }}
union all
select
    {{ pilo_star(ref('stg_contacts_non_commandeurs')) }}
from {{ ref('stg_contacts_non_commandeurs') }}
