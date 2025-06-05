select
    emi_ctr_id,
    emi_motif_sortie_id,
    sum(emi_nb_heures_travail) as emi_nb_heures_travail
from {{ ref('fluxIAE_EtatMensuelIndiv_v2') }}
group by
    emi_ctr_id,
    emi_motif_sortie_id
