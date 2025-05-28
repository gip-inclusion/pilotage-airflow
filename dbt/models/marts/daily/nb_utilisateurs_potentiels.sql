select
    i."région",
    split_part(i."nom_département", ' ', 1) as "département_num",
    i."nom_département"                     as "département",
    'institution'                           as type_utilisateur,
    i.type                                  as profil,
    count(*)                                as potentiel
from {{ source('emplois', 'institutions') }} as i
-- DDETS GEIQ ne sont pas dans la cible
where i.type != 'DDETS GEIQ'
group by i."région", i."nom_département", i.type

union all

select
    o."région_org"                 as "région",
    split_part(o.dept_org, ' ', 1) as "département_num",
    o.dept_org                     as "département",
    'prescripteur'                 as type_utilisateur,
    o.type                         as profil,
    count(*)                       as potentiel
from {{ ref('organisations') }} as o
where o.dept_org is not null and o.habilitation = 'Prescripteur habilité'
group by o."région_org", o.dept_org, o.type

union all

select
    s."région",
    split_part(s."nom_département", ' ', 1) as "département_num",
    s."nom_département"                     as "département",
    'siae'                                  as type_utilisateur,
    s.type                                  as profil,
    count(*)                                as potentiel
from {{ source('emplois', 'structures') }} as s
where s."nom_département" is not null and s.active = 1 and type in ('ACI', 'AI', 'EI', 'EITI', 'ETTI')
group by s."région", s."nom_département", s.type
