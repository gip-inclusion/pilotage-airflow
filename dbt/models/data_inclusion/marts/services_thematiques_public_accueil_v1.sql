select
    {{ pilo_star(source('data_inclusion','services_v1')) }},
    service_thematique,
    service_public,
    service_mode_accueil
from {{ source('data_inclusion', 'services_v1') }}
left join lateral unnest(thematiques) as service_thematique on true
left join lateral unnest(publics) as service_public on true
left join lateral unnest(modes_accueil) as service_mode_accueil on true
