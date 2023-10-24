select
    s.id                      as id,
    s.nom                     as nom,
    s.type                    as type_struct,
    s.siret                   as siret,
    s.active                  as active,
    s.ville                   as ville,
    s."département"           as "département",
    s."nom_département"       as "nom_département",
    s."région"                as "région",
    insee_geo.nom_zone_emploi as bassin_d_emploi,
    s.nom_complet             as "nom_structure_complet"
from
    {{ source('emplois', 'structures') }} as s
left join
    {{ ref('stg_insee_appartenance_geo_communes') }} as insee_geo
    on ltrim(s.code_commune, '0') = insee_geo.code_insee
