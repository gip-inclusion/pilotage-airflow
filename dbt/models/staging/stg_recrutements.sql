select
    contrat_id_pph,
    min(contrat_id_ctr)           as id_premier_contrat,
    min(contrat_date_embauche)    as date_recrutement,
    max(contrat_date_fin_contrat) as date_fin_recrutement,
    max(contrat_type_contrat)     as nb_reconductions
from {{ ref('stg_contrats') }}
group by
    contrat_id_pph,
    id_recrutement