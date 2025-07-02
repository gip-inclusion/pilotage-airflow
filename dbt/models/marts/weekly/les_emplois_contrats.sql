select
    salarie.hash_nir,
    pass_ag."hash_numéro_pass_iae"
from {{ source("emplois","pass_agréments") }} as pass_ag
left join {{ source("fluxIAE","fluxIAE_Salarie") }} as salarie
    on pass_ag."hash_numéro_pass_iae" = salarie."hash_numéro_pass_iae"

where pass_ag.type != 'Agrément PE' and pass_ag.type != 'Agrément PE via ITOU (non 99999)' and salarie.hash_nir is not null

    contrat_id_structure,
    contrat_mesure_disp_code,
    contrat_id_ctr,
    contrat_type_contrat,
    contrat_date_sortie_definitive,
    contrat_date_embauche,
    contrat_date_fin_contrat,
    contrat_id_pph
from {{ ref("fluxIAE_ContratMission_v2") }}
left join
