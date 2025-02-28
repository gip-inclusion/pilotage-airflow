select
 --   ctr.contrat_parent_id,
    ctr.contrat_id_pph,
    ctr.contrat_id_structure,
    ctr.contrat_mesure_disp_code
    --    min(ctr.contrat_date_embauche)          as contrat_date_embauche,
--    max(ctr.contrat_date_sortie_definitive) as contrat_date_sortie_definitive,
--    max(ctr.contrat_date_fin_contrat)       as contrat_date_fin_contrat,
--    max(motif_sortie)                        as motif_sortie
from {{ ref("eph_stg_contrats") }} as ctr
--group by ctr.contrat_parent_id, contrat_id_pph, ctr.contrat_id_structure, ctr.contrat_mesure_disp_code
