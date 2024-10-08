select
    s.id,
    s.id_asp,
    s.nom,
    s.type                    as type_struct,
    s.siret,
    s.active,
    s.ville,
    s."département",
    s."nom_département",
    s."région",
    insee_geo.nom_zone_emploi as bassin_d_emploi,
    s.nom_complet             as "nom_structure_complet",
    case
        when s.convergence_france = 0 then 'Non'
        else 'Oui'
    end                       as structure_convergence
from
    {{ ref('structures') }} as s
left join
    {{ ref('stg_insee_appartenance_geo_communes') }} as insee_geo
    on ltrim(s.code_commune, '0') = insee_geo.code_insee
