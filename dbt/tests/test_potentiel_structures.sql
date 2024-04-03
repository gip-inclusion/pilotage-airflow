with nb_structures_meres as (
    select
        (
            select count(*)
            from structures
            where source != 'Utilisateur (Antenne)' and "nom_département" is not null
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
