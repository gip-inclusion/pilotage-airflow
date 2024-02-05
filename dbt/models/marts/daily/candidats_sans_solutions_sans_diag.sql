select
    distinct
from {{ ref('stg_candidatures_candidats_reels') }} as c
where c.diagnostic_valide = 0
