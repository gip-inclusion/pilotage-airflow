select
    {{ pilo_star(ref('stg_candidats_candidatures'), except=["date_embauche"]) }},
    max(date_embauche) as derniere_date_embauche
from {{ ref('stg_candidats_candidatures') }}
group by
    {{ pilo_star(ref('stg_candidats_candidatures'), except=["date_embauche"]) }}
