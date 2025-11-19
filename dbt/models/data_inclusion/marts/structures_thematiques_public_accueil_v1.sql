select
    {{ pilo_star(source('data_inclusion','structures_v1'), relation_alias = 'structures') }},
    reseau_porteur,
    services.service_thematique,
    services.service_public,
    services.service_mode_accueil,
    geo.nom_region,
    geo.code_region,
    geo.nom_departement,
    lpad(geo.code_dept, 2, '0') as code_dept
from {{ source('data_inclusion', 'structures_v1') }} as structures
left join lateral unnest(reseaux_porteurs) as reseau_porteur on true
left join {{ ref('services_thematiques_public_accueil_v1') }} as services
    on structures.id = services.structure_id
left join stg_insee_appartenance_geo_communes as geo
    on ltrim(structures.code_insee, '0') = geo.code_insee
