select
    ctr.contrat_parent_id,
    ctr.contrat_id_pph,
    ctr.contrat_id_structure,
    ctr.contrat_mesure_disp_code,
    min(ctr.contrat_date_embauche)          as contrat_date_embauche,
    max(ctr.contrat_date_sortie_definitive) as contrat_date_sortie_definitive
from {{ ref("eph_stg_contrats") }} as ctr
group by ctr.contrat_parent_id, contrat_id_pph, ctr.contrat_id_structure, ctr.contrat_mesure_disp_code
