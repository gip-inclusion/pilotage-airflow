select
    emi.emi_pph_id,
    emi.emi_ctr_id,
    min(emi.date_emi) as date_recrutement_reelle
from {{ ref("fluxIAE_EtatMensuelIndiv_v2") }} as emi
where
    emi.emi_nb_heures_travail > 0
group by
    emi.emi_ctr_id,
    emi.emi_pph_id
