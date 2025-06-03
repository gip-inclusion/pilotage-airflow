select
    ctr.contrat_id_ctr,
    sum(case
        when ctr.contrat_type_contrat = 0 then 1
        else 0
    end) over (
        partition by ctr.contrat_id_pph, ctr.contrat_mesure_disp_code
        order by ctr.contrat_date_embauche
    ) as groupe_contrat
from {{ ref('fluxIAE_ContratMission_v2') }} as ctr
