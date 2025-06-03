with nb_contrat as (
    select
        (
            select count(distinct contrat_id_ctr)
            from {{ ref('stg_contrats') }}
            where extract(year from contrat_date_embauche) >= 2021 and num_reconduction = 0
        ) as contrats_stg,
        (
            select count(distinct contrat_id_ctr)
            from {{ ref('fluxIAE_ContratMission_v2') }}
            where extract(year from contrat_date_embauche) >= 2021 and contrat_type_contrat = 0
        ) as contrats
)

select *
from nb_contrat
where contrats_stg != contrats
