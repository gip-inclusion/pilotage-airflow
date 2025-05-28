with nb_structures_meres as (
    select
        (
            select count(*)
            from structures
            where "nom_département" is not null and active = 1 and type in ('ACI', 'AI', 'EI', 'EITI', 'ETTI')
        ) as structures_meres,
        (
            select sum(potentiel)
            from nb_utilisateurs_potentiels
            where type_utilisateur = 'siae'
        ) as structures_mere_potentiel
)

select *
from nb_structures_meres
where structures_meres != structures_mere_potentiel
