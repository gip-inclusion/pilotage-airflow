select
    emi.emi_pph_id,
    emi.emi_afi_id,
    emi.emi_ctr_id,
    emi.emi_sme_annee,
    sum(emi.emi_nb_heures_travail) as emi_nb_heures_travail
from {{ ref('fluxIAE_EtatMensuelIndiv_v2') }} as emi
group by
    emi.emi_pph_id,
    emi.emi_afi_id,
    emi.emi_ctr_id,
    emi.emi_sme_annee
