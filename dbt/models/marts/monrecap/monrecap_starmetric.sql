select
    {{ pilo_star(ref('stg_starmetric'),relation_alias="starm") }},
    rep.total_answers
from {{ ref('stg_starmetric') }} as starm
left join {{ ref('stg_total_reponses') }} as rep
    on
        extract(year from starm.derniere_reponse_barometre) = rep.annee
        and extract(month from starm.derniere_reponse_barometre) = rep.mois
