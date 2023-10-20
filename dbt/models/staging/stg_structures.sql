select
    s.id                      as id,
    s.siret                   as siret,
    s.active                  as active,
    s.ville                   as ville,
    insee_geo.nom_zone_emploi as bassin_emploi_structure,
    s.nom_complet             as "nom_structure_complet"
from
    {{ source('emplois', 'structures') }} as s
left join
    {{ ref('stg_insee_appartenance_geo_communes') }} as insee_geo
    on ltrim(s.code_commune, '0') = insee_geo.code_insee
