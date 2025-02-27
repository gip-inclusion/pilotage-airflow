select
    {{ pilo_star(ref('stg_contrats')) }},
    first_value(ctr.contrat_id_pph) over (partition by ctr.contrat_id_pph, groupe_contrat order by ctr.contrat_date_embauche) as contrat_parent_id
from {{ ref('stg_contrats') }} as ctr
