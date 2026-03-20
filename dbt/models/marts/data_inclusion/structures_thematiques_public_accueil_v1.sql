select
    {{ pilo_star(ref('structures_v1'), relation_alias = 'structures') }},
    reseau_porteur,
    services.service_thematique,
    services.service_public,
    services.service_mode_accueil,
    geo.nom_region,
    geo.code_region,
    geo.nom_departement,
    case
        when geo.code_dept like '%97%' then lpad(geo.code_dept, 3, '0')
        else lpad(geo.code_dept, 2, '0')
    end as code_dept
from {{ ref('structures_v1') }} as structures
left join lateral unnest(reseaux_porteurs) as reseau_porteur on true
left join {{ ref('services_thematiques_public_accueil_v1') }} as services
    on structures.id = services.structure_id
left join {{ ref('stg_insee_appartenance_geo_communes') }} as geo
    on ltrim(structures.code_insee, '0') = geo.code_insee
