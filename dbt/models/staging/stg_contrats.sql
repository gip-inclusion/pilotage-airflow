select
    {{ pilo_star(ref('fluxIAE_ContratMission_v2')) }},
    sum(case
        when contrat_type_contrat = 0 then 1
        else 0
    end) over (partition by contrat_id_pph order by contrat_date_embauche) as id_recrutement
from {{ ref('fluxIAE_ContratMission_v2') }}
