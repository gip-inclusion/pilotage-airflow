select
    id_candidat,
    max(date_candidature) as date_derniere_candidature
from
    {{ ref('stg_candidatures') }}
group by
    id_candidat
