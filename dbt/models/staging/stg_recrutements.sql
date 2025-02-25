select
    contrat_id_pph,
    min(contrat_id_ctr)                 as id_premier_contrat,
    max(contrat_id_ctr)                 as id_derniere_reconduction,
    min(contrat_date_embauche)          as date_recrutement,
    max(contrat_date_fin_contrat)       as date_fin_recrutement,
    max(contrat_date_sortie_definitive) as date_sortie_definitive,
    max(num_reconduction)               as nb_reconductions
from {{ ref('stg_contrats') }}
--because we consider only contracts starting in 2021
where extract(year from contrat_date_embauche) >= 2021
group by
    contrat_id_pph,
    groupe_contrat
