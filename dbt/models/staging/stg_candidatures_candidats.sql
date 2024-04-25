select
    id_candidat,
    min(date_candidature) as date_premiere_candidature,
    max(date_candidature) as date_derniere_candidature
from
    {{ ref('stg_candidatures') }}
group by
    id_candidat
