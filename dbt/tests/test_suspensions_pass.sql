with suspensions_en_cours as (
    select
        count(*)                            as nombre_lignes,
        count(distinct "id_pass_agrément") as nb_pass_suspension_en_cours
    from {{ ref('suspensions_pass') }}
    where suspension_en_cours = 'Oui'
)

select *
from suspensions_en_cours
where nombre_lignes != nb_pass_suspension_en_cours
