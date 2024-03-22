select {{ pilo_star(ref('stg_candidats_candidatures')) }}
from {{ ref('stg_candidats_candidatures') }}
where date_candidature >= current_date - interval '6 months'
