select
    {{ pilo_star(source('emplois','structures_v0'), except=['source', 'ville'], relation_alias='struct') }},
    insee.libelle_commune as ville,
    grp_strct.groupe      as categorie_structure,
    case
        when struct.type = 'OPCS' and struct.source = 'Utilisateur (Antenne)' then 'Utilisateur (OPCS)'
        when struct.type = 'OPCS' and struct.source = 'Staff Itou' then 'Staff Itou (OPCS)'
        else struct.source
    end                   as source
from
    {{ source('emplois','structures_v0') }} as struct
left join
    {{ ref('groupes_structures') }} as grp_strct
    on grp_strct.structure = struct.type
left join
    {{ ref('insee_communes') }} as insee
    on coalesce(ltrim(struct.code_commune, '0'), ltrim(struct.code_commune_c1, '0')) = insee.code_insee
