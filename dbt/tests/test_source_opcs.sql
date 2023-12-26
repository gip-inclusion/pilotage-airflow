with nb_opcs as (
    select
        (
            select count(*)
            from structures_v0
            where type = 'OPCS'
        ) as nb_opcs_structs,
        (
            select count(*)
            from structures
            where source = 'Staff Itou (OPCS)' or source = 'Utilisateur (OPCS)'
        ) as nb_opcs_sources
)

select *
from nb_opcs
where nb_opcs_structs != nb_opcs_sources
