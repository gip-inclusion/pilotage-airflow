/* historique de 2 ans */
select
    (max(emi.emi_sme_annee) - 2) as annee_en_cours_2,
    max(emi.emi_sme_annee)       as annee_en_cours
from
    {{ ref('fluxIAE_EtatMensuelIndiv_v2') }} as emi
