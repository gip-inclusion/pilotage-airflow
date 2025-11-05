select
    {{ pilo_star(source('gps','gps_log_data'), relation_alias='gps' ) }},
    util.email,
    util.type,
    util.prenom,
    util.nom,
    util.id_structure,
    util.id_organisation,
    util.id_institution,
    s.nom                                                                       as nom_structure,
    s.type                                                                      as type_structure,
    s.siret,
    s."nom_département"                                                         as departement_structure,
    s."région"                                                                  as region_structure,
    org.nom                                                                     as nom_org,
    org.type                                                                    as type_org,
    org.type_complet,
    org.dept_org,
    org."région_org",
    instit.nom                                                                  as nom_instit,
    instit.type                                                                 as type_instit,
    instit."nom_département"                                                    as departement_instit,
    instit."région"                                                             as region_instit,
    -- we add this column to allow a simple filter by dpt with metabase. We accept the fact that the dpt can be imperfect
    coalesce(s."nom_département", org.dept_org, instit."nom_département", null) as departement_global
from {{ source('gps', 'gps_log_data') }} as gps
left join {{ ref('utilisateurs') }} as util
    on gps.user_id = util.id
left join {{ ref('structures') }} as s
    on util.id_structure = s.id
left join {{ ref('organisations') }} as org
    on util.id_organisation = org.id
left join {{ source('emplois','institutions') }} as instit
    on util.id_institution = instit.id
