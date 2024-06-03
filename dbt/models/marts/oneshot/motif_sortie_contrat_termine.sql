select
    emi.emi_pph_id          as pph_id,
    emi.emi_ctr_id          as ctr_id,
    emi.emi_date_fin_reelle as date_fin_reelle,
    rms.rms_libelle         as motif_de_sortie
from {{ ref("fluxIAE_EtatMensuelIndiv_v2") }} as emi
left join {{ ref("fluxIAE_RefMotifSort_v2") }} as rms
    on emi.emi_motif_sortie_id = rms.rms_id
where emi.emi_date_fin_reelle is not null
group by
    emi.emi_pph_id,
    emi.emi_ctr_id,
    emi.emi_date_fin_reelle,
    emi.emi_motif_sortie_id,
    rms.rms_libelle
