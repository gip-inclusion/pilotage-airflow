select
    {{ pilo_star(source('emplois','structures_v0'), except=['source', 'ville'], relation_alias='struct') }},
    dim_commune.nom_commune     as ville,
    dim_commune.nom_zone_emploi as bassin_d_emploi,
    grp_strct.groupe            as categorie_structure,
    case
        when struct.source = 'Export ASP' then strct_asp.nom_epci_structure
        else dim_commune.nom_epci
    end                         as nom_epci_structure,
    case
        when struct.type = 'OPCS' and struct.source = 'Utilisateur (Antenne)' then 'Utilisateur (OPCS)'
        when struct.type = 'OPCS' and struct.source = 'Staff Itou' then 'Staff Itou (OPCS)'
        else struct.source
    end                         as source
from
    {{ source('emplois','structures_v0') }} as struct
left join
    {{ ref('groupes_structures') }} as grp_strct
    on struct.type = grp_strct.structure
left join
    {{ ref('dim_commune') }}
    on coalesce(struct.code_commune, struct.code_commune_c1) = dim_commune.code_commune_insee
left join
    {{ ref('fluxIAE_Structure_v2') }} as strct_asp
    on struct.id_asp = strct_asp.structure_id_siae
