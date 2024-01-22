select distinct
    ctr.contrat_id_pph,
    ctr.contrat_id_ctr,
    ctr.contrat_date_embauche,
    ctr.contrat_date_fin_contrat,
    ctr.contrat_type_contrat,
    ctr.contrat_date_sortie_definitive,
    sum(case
        when ctr.contrat_type_contrat = 0 then 1
        else 0
    end) over (partition by ctr.contrat_id_pph order by ctr.contrat_date_embauche) as id_recrutement
from {{ ref('fluxIAE_EtatMensuelIndiv_v2') }} as emi
left join {{ ref('fluxIAE_ContratMission_v2') }} as ctr
    on ctr.contrat_id_ctr = emi.emi_ctr_id
