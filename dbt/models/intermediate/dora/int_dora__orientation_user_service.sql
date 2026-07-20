select
    {{ dbt_utils.star(relation_alias='orientations', from=ref('stg_dora__orientation'), prefix='orientation_') }},
    {{ dbt_utils.star(relation_alias='dora_users', from=ref('stg_dora__user'), prefix='user_') }},
    {{ dbt_utils.star(relation_alias='services', from=ref('int_dora__service_structure')) }}
from {{ ref('stg_dora__orientation') }} as orientations
-- left join pour considérer les orientations faites par des users supprimés
left join {{ ref('stg_dora__user') }} as dora_users
    on orientations.prescriber_id = dora_users.id
-- left join pour considérer les cas suivants : 
-- orientations faites sur des services sans service_id (ont un di_service_id)
-- orientations faites sur des services non rattachés à une structure
left join {{ ref('int_dora__service_structure') }} as services
    on orientations.service_id = services.service_id
