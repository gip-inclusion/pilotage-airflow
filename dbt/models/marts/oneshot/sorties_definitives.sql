select
    ctr.contrat_id_pph,
    ctr.contrat_id_structure,
    ref_mesure.type_structure_emplois       as type_structure,
    max(ctr.contrat_date_sortie_definitive) as date_sortie
--    array_agg(motif_sortie.rms_libelle) as motifs_sortie
from {{ ref('fluxIAE_ContratMission_v2') }} as ctr
left join {{ ref('ref_mesure_dispositif_asp') }} as ref_mesure
    on ref_mesure.af_mesure_dispositif_code = ctr.contrat_mesure_disp_code
where ctr.contrat_date_sortie_definitive is not null
group by
    ctr.contrat_id_pph,
    ctr.contrat_id_structure,
    ref_mesure.type_structure_emplois
